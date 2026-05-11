package raftstore

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	capsuleauth "github.com/feichai0017/NoKV/fsmeta/runtime/capsuleauth"
)

const (
	defaultCapsuleSubmitRetries      = 3
	defaultCapsuleSubmitRetryBackoff = 20 * time.Millisecond
)

type capsuleGrantProvider interface {
	HolderID() string
	Acquire(context.Context, compile.AuthorityScope) (capsuleauth.AuthorityGrant, bool, error)
}

type RemoteCapsuleCommitterConfig struct {
	Authority     capsuleGrantProvider
	Witnesses     []fscapsule.WitnessReplica
	Quorum        int
	SubmitRetries int
	RetryBackoff  time.Duration
	Now           func() time.Time
}

// RemoteCapsuleCommitter is the fsmeta runtime bridge from compiler deltas to
// remote durable witnesses. It keeps an in-process read overlay until seal/apply
// lands; this is the experimental fast path used for end-to-end benchmarking.
type RemoteCapsuleCommitter struct {
	authority capsuleGrantProvider
	witnesses []fscapsule.WitnessReplica
	quorum    int
	retries   int
	backoff   time.Duration
	now       func() time.Time

	mu      sync.Mutex
	holders map[uint64]*fscapsule.Holder

	overlayMu sync.RWMutex
	overlay   map[string]runtimeCapsuleOverlayEntry

	commitTotal atomic.Uint64
	errorTotal  atomic.Uint64
	retryTotal  atomic.Uint64
}

type runtimeCapsuleOverlayEntry struct {
	key    []byte
	value  []byte
	delete bool
}

func NewRemoteCapsuleCommitter(cfg RemoteCapsuleCommitterConfig) (*RemoteCapsuleCommitter, error) {
	if cfg.Authority == nil || cfg.Authority.HolderID() == "" || len(cfg.Witnesses) == 0 {
		return nil, errCapsuleCommitterInvalid
	}
	witnesses := make([]fscapsule.WitnessReplica, 0, len(cfg.Witnesses))
	seen := make(map[string]struct{}, len(cfg.Witnesses))
	for _, witness := range cfg.Witnesses {
		if witness == nil || witness.ID() == "" {
			return nil, errCapsuleCommitterInvalid
		}
		if _, ok := seen[witness.ID()]; ok {
			return nil, errCapsuleCommitterInvalid
		}
		seen[witness.ID()] = struct{}{}
		witnesses = append(witnesses, witness)
	}
	quorum := cfg.Quorum
	if quorum == 0 {
		quorum = len(witnesses)/2 + 1
	}
	if quorum <= 0 || quorum > len(witnesses) {
		return nil, errCapsuleCommitterInvalid
	}
	retries := cfg.SubmitRetries
	if retries == 0 {
		retries = defaultCapsuleSubmitRetries
	}
	if retries < 0 {
		return nil, errCapsuleCommitterInvalid
	}
	backoff := cfg.RetryBackoff
	if backoff == 0 {
		backoff = defaultCapsuleSubmitRetryBackoff
	}
	if backoff < 0 {
		return nil, errCapsuleCommitterInvalid
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &RemoteCapsuleCommitter{
		authority: cfg.Authority,
		witnesses: witnesses,
		quorum:    quorum,
		retries:   retries,
		backoff:   backoff,
		now:       now,
		holders:   make(map[uint64]*fscapsule.Holder),
		overlay:   make(map[string]runtimeCapsuleOverlayEntry),
	}, nil
}

func (c *RemoteCapsuleCommitter) CommitCapsule(ctx context.Context, id fscapsule.OperationID, delta compile.SemanticDelta) (fscapsule.CapsuleSeal, error) {
	if c == nil || c.authority == nil {
		return fscapsule.CapsuleSeal{}, errCapsuleCommitterInvalid
	}
	grant, owned, err := c.authority.Acquire(ctx, delta.Authority)
	if err != nil {
		c.errorTotal.Add(1)
		return fscapsule.CapsuleSeal{}, err
	}
	if !owned {
		c.errorTotal.Add(1)
		return fscapsule.CapsuleSeal{}, errCapsuleAuthorityNotHeld
	}
	holder, err := c.holderForGrant(grant)
	if err != nil {
		c.errorTotal.Add(1)
		return fscapsule.CapsuleSeal{}, err
	}
	if err := c.submitWithRetry(ctx, holder, id, delta); err != nil {
		c.errorTotal.Add(1)
		return fscapsule.CapsuleSeal{}, err
	}
	if err := c.addOverlay(delta); err != nil {
		c.errorTotal.Add(1)
		return fscapsule.CapsuleSeal{}, err
	}
	c.commitTotal.Add(1)
	return fscapsule.CapsuleSeal{}, nil
}

func (c *RemoteCapsuleCommitter) holderForGrant(grant capsuleauth.AuthorityGrant) (*fscapsule.Holder, error) {
	if !grant.Valid() || grant.HolderID != c.authority.HolderID() {
		return nil, errCapsuleCommitterInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if holder := c.holders[grant.EpochID]; holder != nil {
		return holder, nil
	}
	holder, err := fscapsule.NewHolder(fscapsule.HolderConfig{
		EpochID:   grant.EpochID,
		HolderID:  grant.HolderID,
		Witnesses: c.witnesses,
		Quorum:    c.quorum,
		Now:       c.now,
	})
	if err != nil {
		return nil, err
	}
	c.holders[grant.EpochID] = holder
	return holder, nil
}

func (c *RemoteCapsuleCommitter) submitWithRetry(ctx context.Context, holder *fscapsule.Holder, id fscapsule.OperationID, delta compile.SemanticDelta) error {
	var last error
	attempts := c.retries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		_, err := holder.Submit(ctx, id, delta)
		if err == nil {
			return nil
		}
		last = err
		if !errors.Is(err, fscapsule.ErrWitnessQuorumUnavailable) || attempt == attempts-1 {
			break
		}
		c.retryTotal.Add(1)
		if !sleepContext(ctx, c.backoff) {
			return ctx.Err()
		}
	}
	return last
}

func (c *RemoteCapsuleCommitter) GetCapsuleOverlay(key []byte) ([]byte, bool, bool) {
	if c == nil {
		return nil, false, false
	}
	c.overlayMu.RLock()
	entry, ok := c.overlay[string(key)]
	c.overlayMu.RUnlock()
	if !ok {
		return nil, false, false
	}
	return runtimeCloneBytes(entry.value), entry.delete, true
}

func (c *RemoteCapsuleCommitter) ScanCapsuleOverlay(start []byte, limit uint32) []fscapsule.OverlayKV {
	if c == nil || limit == 0 {
		return nil
	}
	c.overlayMu.RLock()
	keys := make([]string, 0, len(c.overlay))
	for key := range c.overlay {
		if bytes.Compare([]byte(key), start) >= 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > int(limit) {
		keys = keys[:limit]
	}
	out := make([]fscapsule.OverlayKV, 0, len(keys))
	for _, key := range keys {
		entry := c.overlay[key]
		out = append(out, fscapsule.OverlayKV{
			Key:    runtimeCloneBytes(entry.key),
			Value:  runtimeCloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	c.overlayMu.RUnlock()
	return out
}

func (c *RemoteCapsuleCommitter) Stats() map[string]any {
	if c == nil {
		return map[string]any{
			"commit_total":  uint64(0),
			"error_total":   uint64(0),
			"retry_total":   uint64(0),
			"overlay_keys":  0,
			"holders":       0,
			"witness_count": 0,
			"quorum":        0,
		}
	}
	c.overlayMu.RLock()
	overlayKeys := len(c.overlay)
	c.overlayMu.RUnlock()
	c.mu.Lock()
	holders := len(c.holders)
	c.mu.Unlock()
	return map[string]any{
		"commit_total":  c.commitTotal.Load(),
		"error_total":   c.errorTotal.Load(),
		"retry_total":   c.retryTotal.Load(),
		"overlay_keys":  overlayKeys,
		"holders":       holders,
		"witness_count": len(c.witnesses),
		"quorum":        c.quorum,
	}
}

func (c *RemoteCapsuleCommitter) Close() {}

func (c *RemoteCapsuleCommitter) addOverlay(delta compile.SemanticDelta) error {
	c.overlayMu.Lock()
	defer c.overlayMu.Unlock()
	for _, effect := range delta.WriteEffects {
		if len(effect.Key) == 0 {
			return errCapsuleCommitterInvalid
		}
		entry := runtimeCapsuleOverlayEntry{key: runtimeCloneBytes(effect.Key)}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return errCapsuleCommitterInvalid
			}
			entry.value = runtimeCloneBytes(effect.Value)
		case compile.EffectDelete:
			entry.delete = true
		default:
			return errCapsuleCommitterInvalid
		}
		c.overlay[string(effect.Key)] = entry
	}
	return nil
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func runtimeCloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
