package peras

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type OverlayKV struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type BufferedCommitterConfig struct {
	Holder        *Holder
	Snapshot      WitnessSnapshotSource
	Versions      VersionAllocator
	ReplayDB      InternalEntryApplier
	BatchSize     int
	FlushInterval time.Duration
	ApplyHook     func(ReplayPlan, ApplyStats)
}

type BufferedCommitter struct {
	holder   *Holder
	snapshot WitnessSnapshotSource
	versions VersionAllocator
	replayDB InternalEntryApplier
	hook     func(ReplayPlan, ApplyStats)

	batchSize     int
	flushInterval time.Duration
	stop          chan struct{}
	mu            sync.Mutex
	flushMu       sync.Mutex

	overlayMu sync.RWMutex
	overlay   map[string]overlayEntry

	commitTotal atomic.Uint64
	flushTotal  atomic.Uint64
	applyTotal  atomic.Uint64
	errorTotal  atomic.Uint64
}

type overlayEntry struct {
	opID   OperationID
	key    []byte
	value  []byte
	delete bool
}

func NewBufferedCommitter(cfg BufferedCommitterConfig) (*BufferedCommitter, error) {
	if cfg.Holder == nil || cfg.Snapshot == nil || cfg.Versions == nil || cfg.ReplayDB == nil {
		return nil, ErrHolderConfigInvalid
	}
	c := &BufferedCommitter{
		holder:        cfg.Holder,
		snapshot:      cfg.Snapshot,
		versions:      cfg.Versions,
		replayDB:      cfg.ReplayDB,
		hook:          cfg.ApplyHook,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		stop:          make(chan struct{}),
		overlay:       make(map[string]overlayEntry),
	}
	if c.flushInterval > 0 {
		go c.flushLoop()
	}
	return c, nil
}

func (c *BufferedCommitter) CommitPeras(ctx context.Context, id OperationID, delta compile.SemanticDelta) (PerasSeal, error) {
	if c == nil || c.holder == nil {
		return PerasSeal{}, ErrHolderConfigInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.holder.Submit(ctx, id, delta); err != nil {
		c.errorTotal.Add(1)
		return PerasSeal{}, err
	}
	if err := c.addOverlay(id, delta); err != nil {
		c.errorTotal.Add(1)
		return PerasSeal{}, err
	}
	c.commitTotal.Add(1)
	if c.batchSize > 0 && c.holder.Pending() >= c.batchSize {
		go func() {
			if err := c.Flush(context.Background()); err != nil {
				c.errorTotal.Add(1)
			}
		}()
	}
	return PerasSeal{}, nil
}

func (c *BufferedCommitter) Flush(ctx context.Context) error {
	if c == nil || c.holder == nil || c.snapshot == nil || c.versions == nil || c.replayDB == nil {
		return ErrHolderConfigInvalid
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := c.holder.PendingIDs()
	if len(pending) == 0 {
		return nil
	}
	firstVersion, err := c.versions.ReserveTimestamp(ctx, uint64(len(pending)))
	if err != nil {
		return err
	}
	snapshot, err := c.snapshot.Probe(ctx, c.holder.EpochID())
	if err != nil {
		return err
	}
	seal, err := c.holder.BuildPendingSealWithVersions(firstVersion, snapshot)
	if err != nil {
		return err
	}
	if len(seal.Certificates) != len(pending) {
		return ErrInvalidPerasSeal
	}
	plan, err := BuildReplayPlan(seal)
	if err != nil {
		return err
	}
	store, err := NewMVCCReplayStoreForPlan(c.replayDB, plan)
	if err != nil {
		return err
	}
	stats, err := ApplyReplayPlan(store, plan)
	if err != nil {
		return err
	}
	if c.hook != nil {
		c.hook(plan, stats)
	}
	if err := c.holder.MarkSealApplied(seal); err != nil {
		return err
	}
	c.removeOverlayForPlan(plan)
	c.flushTotal.Add(1)
	c.applyTotal.Add(stats.Operations)
	return nil
}

func (c *BufferedCommitter) GetPerasOverlay(key []byte) (value []byte, deleted bool, ok bool) {
	if c == nil {
		return nil, false, false
	}
	c.overlayMu.RLock()
	entry, ok := c.overlay[string(key)]
	c.overlayMu.RUnlock()
	if !ok {
		return nil, false, false
	}
	return cloneBytes(entry.value), entry.delete, true
}

func (c *BufferedCommitter) ScanPerasOverlay(start []byte, limit uint32) []OverlayKV {
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
	out := make([]OverlayKV, 0, len(keys))
	for _, key := range keys {
		entry := c.overlay[key]
		out = append(out, OverlayKV{
			Key:    cloneBytes(entry.key),
			Value:  cloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	c.overlayMu.RUnlock()
	return out
}

func (c *BufferedCommitter) Stats() map[string]any {
	if c == nil {
		return map[string]any{
			"commit_total": uint64(0),
			"flush_total":  uint64(0),
			"apply_total":  uint64(0),
			"error_total":  uint64(0),
			"overlay_keys": 0,
			"pending":      0,
		}
	}
	c.overlayMu.RLock()
	overlayKeys := len(c.overlay)
	c.overlayMu.RUnlock()
	return map[string]any{
		"commit_total": c.commitTotal.Load(),
		"flush_total":  c.flushTotal.Load(),
		"apply_total":  c.applyTotal.Load(),
		"error_total":  c.errorTotal.Load(),
		"overlay_keys": overlayKeys,
		"pending":      c.holder.Pending(),
	}
}

func (c *BufferedCommitter) Close() {
	if c == nil || c.stop == nil {
		return
	}
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
}

func (c *BufferedCommitter) flushLoop() {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.Flush(context.Background()); err != nil {
				c.errorTotal.Add(1)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *BufferedCommitter) addOverlay(id OperationID, delta compile.SemanticDelta) error {
	c.overlayMu.Lock()
	defer c.overlayMu.Unlock()
	for _, effect := range delta.WriteEffects {
		if len(effect.Key) == 0 {
			return ErrInvalidPerasSeal
		}
		entry := overlayEntry{
			opID: id,
			key:  cloneBytes(effect.Key),
		}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return ErrInvalidPerasSeal
			}
			entry.value = cloneBytes(effect.Value)
		case compile.EffectDelete:
			entry.delete = true
		default:
			return ErrInvalidPerasSeal
		}
		c.overlay[string(effect.Key)] = entry
	}
	return nil
}

func (c *BufferedCommitter) removeOverlayForPlan(plan ReplayPlan) {
	c.overlayMu.Lock()
	defer c.overlayMu.Unlock()
	for _, op := range plan.Operations {
		for _, mutation := range op.Mutations {
			entry, ok := c.overlay[string(mutation.Key)]
			if ok && entry.opID == op.OpID {
				delete(c.overlay, string(mutation.Key))
			}
		}
	}
}
