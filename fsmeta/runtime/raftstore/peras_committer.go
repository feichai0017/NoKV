package raftstore

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
)

const (
	defaultPerasSubmitRetries      = 3
	defaultPerasSubmitRetryBackoff = 20 * time.Millisecond
)

type perasGrantProvider interface {
	HolderID() string
	Acquire(context.Context, compile.AuthorityScope) (perasauth.AuthorityGrant, bool, error)
}

type RemotePerasCommitterConfig struct {
	Authority     perasGrantProvider
	Witnesses     []fsperas.WitnessReplica
	Quorum        int
	SubmitRetries int
	RetryBackoff  time.Duration
	Now           func() time.Time
}

// RemotePerasCommitter is the fsmeta runtime bridge from compiler deltas to
// remote durable witnesses. It keeps an in-process read overlay until seal/apply
// lands; this is the experimental fast path used for end-to-end benchmarking.
type RemotePerasCommitter struct {
	authority perasGrantProvider
	witnesses []fsperas.WitnessReplica
	quorum    int
	retries   int
	backoff   time.Duration
	now       func() time.Time

	mu      sync.Mutex
	holders map[uint64]*fsperas.Holder

	overlayMu sync.RWMutex
	overlay   map[string]runtimePerasOverlayEntry

	commitTotal atomic.Uint64
	errorTotal  atomic.Uint64
	retryTotal  atomic.Uint64
}

type runtimePerasOverlayEntry struct {
	key    []byte
	value  []byte
	delete bool
}

func NewRemotePerasCommitter(cfg RemotePerasCommitterConfig) (*RemotePerasCommitter, error) {
	if cfg.Authority == nil || cfg.Authority.HolderID() == "" || len(cfg.Witnesses) == 0 {
		return nil, errPerasCommitterInvalid
	}
	witnesses := make([]fsperas.WitnessReplica, 0, len(cfg.Witnesses))
	seen := make(map[string]struct{}, len(cfg.Witnesses))
	for _, witness := range cfg.Witnesses {
		if witness == nil || witness.ID() == "" {
			return nil, errPerasCommitterInvalid
		}
		if _, ok := seen[witness.ID()]; ok {
			return nil, errPerasCommitterInvalid
		}
		seen[witness.ID()] = struct{}{}
		witnesses = append(witnesses, witness)
	}
	quorum := cfg.Quorum
	if quorum == 0 {
		quorum = len(witnesses)/2 + 1
	}
	if quorum <= 0 || quorum > len(witnesses) {
		return nil, errPerasCommitterInvalid
	}
	retries := cfg.SubmitRetries
	if retries == 0 {
		retries = defaultPerasSubmitRetries
	}
	if retries < 0 {
		return nil, errPerasCommitterInvalid
	}
	backoff := cfg.RetryBackoff
	if backoff == 0 {
		backoff = defaultPerasSubmitRetryBackoff
	}
	if backoff < 0 {
		return nil, errPerasCommitterInvalid
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &RemotePerasCommitter{
		authority: cfg.Authority,
		witnesses: witnesses,
		quorum:    quorum,
		retries:   retries,
		backoff:   backoff,
		now:       now,
		holders:   make(map[uint64]*fsperas.Holder),
		overlay:   make(map[string]runtimePerasOverlayEntry),
	}, nil
}

func (c *RemotePerasCommitter) CommitPeras(ctx context.Context, id fsperas.OperationID, delta compile.SemanticDelta) (fsperas.PerasSeal, error) {
	if c == nil || c.authority == nil {
		return fsperas.PerasSeal{}, errPerasCommitterInvalid
	}
	grant, owned, err := c.authority.Acquire(ctx, delta.Authority)
	if err != nil {
		c.errorTotal.Add(1)
		return fsperas.PerasSeal{}, err
	}
	if !owned {
		c.errorTotal.Add(1)
		return fsperas.PerasSeal{}, errPerasAuthorityNotHeld
	}
	holder, err := c.holderForGrant(grant)
	if err != nil {
		c.errorTotal.Add(1)
		return fsperas.PerasSeal{}, err
	}
	if err := c.submitWithRetry(ctx, holder, id, delta); err != nil {
		c.errorTotal.Add(1)
		return fsperas.PerasSeal{}, err
	}
	if err := c.addOverlay(delta); err != nil {
		c.errorTotal.Add(1)
		return fsperas.PerasSeal{}, err
	}
	c.commitTotal.Add(1)
	return fsperas.PerasSeal{}, nil
}

func (c *RemotePerasCommitter) holderForGrant(grant perasauth.AuthorityGrant) (*fsperas.Holder, error) {
	if !grant.Valid() || grant.HolderID != c.authority.HolderID() {
		return nil, errPerasCommitterInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if holder := c.holders[grant.EpochID]; holder != nil {
		return holder, nil
	}
	holder, err := fsperas.NewHolder(fsperas.HolderConfig{
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

func (c *RemotePerasCommitter) submitWithRetry(ctx context.Context, holder *fsperas.Holder, id fsperas.OperationID, delta compile.SemanticDelta) error {
	var last error
	attempts := c.retries + 1
	for attempt := range attempts {
		_, err := holder.Submit(ctx, id, delta)
		if err == nil {
			return nil
		}
		last = err
		if !errors.Is(err, fsperas.ErrWitnessQuorumUnavailable) || attempt == attempts-1 {
			break
		}
		c.retryTotal.Add(1)
		if !sleepContext(ctx, c.backoff) {
			return ctx.Err()
		}
	}
	return last
}

func (c *RemotePerasCommitter) GetPerasOverlay(key []byte) ([]byte, bool, bool) {
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

func (c *RemotePerasCommitter) ScanPerasOverlay(start []byte, limit uint32) []fsperas.OverlayKV {
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
	out := make([]fsperas.OverlayKV, 0, len(keys))
	for _, key := range keys {
		entry := c.overlay[key]
		out = append(out, fsperas.OverlayKV{
			Key:    runtimeCloneBytes(entry.key),
			Value:  runtimeCloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	c.overlayMu.RUnlock()
	return out
}

func (c *RemotePerasCommitter) Stats() map[string]any {
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

func (c *RemotePerasCommitter) Close() {}

func (c *RemotePerasCommitter) addOverlay(delta compile.SemanticDelta) error {
	c.overlayMu.Lock()
	defer c.overlayMu.Unlock()
	for _, effect := range delta.WriteEffects {
		if len(effect.Key) == 0 {
			return errPerasCommitterInvalid
		}
		entry := runtimePerasOverlayEntry{key: runtimeCloneBytes(effect.Key)}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return errPerasCommitterInvalid
			}
			entry.value = runtimeCloneBytes(effect.Value)
		case compile.EffectDelete:
			entry.delete = true
		default:
			return errPerasCommitterInvalid
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
