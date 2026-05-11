package peras

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type OverlayKV struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type BufferedCommitterConfig struct {
	Holder        *Holder
	Versions      VersionAllocator
	ReplayDB      InternalEntryApplier
	BatchSize     int
	FlushInterval time.Duration
	ApplyHook     func(ReplayPlan, ApplyStats)
	SegmentHook   func(PerasSegment, SegmentStats)
}

type BufferedCommitter struct {
	holder      *Holder
	versions    VersionAllocator
	replayDB    InternalEntryApplier
	hook        func(ReplayPlan, ApplyStats)
	segmentHook func(PerasSegment, SegmentStats)

	batchSize     int
	flushInterval time.Duration
	stop          chan struct{}
	mu            sync.Mutex
	flushMu       sync.Mutex

	overlayMu sync.RWMutex
	overlay   map[string]overlayEntry
	known     map[string]bool
	emptyDirs map[string]struct{}

	commitTotal       atomic.Uint64
	flushTotal        atomic.Uint64
	applyTotal        atomic.Uint64
	segmentTotal      atomic.Uint64
	segmentOpsTotal   atomic.Uint64
	segmentEntryTotal atomic.Uint64
	errorTotal        atomic.Uint64

	statsMu          sync.RWMutex
	lastSegmentStats SegmentStats
	lastSegmentRoot  [32]byte
}

type overlayEntry struct {
	opID   OperationID
	key    []byte
	value  []byte
	delete bool
}

func NewBufferedCommitter(cfg BufferedCommitterConfig) (*BufferedCommitter, error) {
	if cfg.Holder == nil || cfg.Versions == nil || cfg.ReplayDB == nil {
		return nil, ErrHolderConfigInvalid
	}
	c := &BufferedCommitter{
		holder:        cfg.Holder,
		versions:      cfg.Versions,
		replayDB:      cfg.ReplayDB,
		hook:          cfg.ApplyHook,
		segmentHook:   cfg.SegmentHook,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		stop:          make(chan struct{}),
		overlay:       make(map[string]overlayEntry),
		known:         make(map[string]bool),
		emptyDirs:     make(map[string]struct{}),
	}
	if c.flushInterval > 0 {
		go c.flushLoop()
	}
	return c, nil
}

func (c *BufferedCommitter) CommitPeras(ctx context.Context, id OperationID, delta compile.SemanticDelta, admission AdmissionFunc) (VisibleAck, error) {
	if c == nil || c.holder == nil {
		return VisibleAck{}, ErrHolderConfigInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := Admit(ctx, delta, admission); err != nil {
		return VisibleAck{}, err
	}
	ack, err := c.holder.Submit(ctx, id, delta)
	if err != nil {
		c.errorTotal.Add(1)
		return VisibleAck{}, err
	}
	if err := c.addOverlay(id, delta); err != nil {
		c.errorTotal.Add(1)
		return VisibleAck{}, err
	}
	c.commitTotal.Add(1)
	if c.batchSize > 0 && c.holder.Pending() >= c.batchSize {
		go func() {
			if err := c.Flush(context.Background()); err != nil {
				c.errorTotal.Add(1)
			}
		}()
	}
	return ack, nil
}

func (c *BufferedCommitter) Flush(ctx context.Context) error {
	if c == nil || c.holder == nil || c.versions == nil || c.replayDB == nil {
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
	plan, _, err := c.holder.BuildPendingReplayPlan(firstVersion)
	if err != nil {
		return err
	}
	if len(plan.Operations) != len(pending) {
		return ErrInvalidPerasSegment
	}
	segment, err := BuildPerasSegmentFromReplayPlan(plan)
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
	segmentStats := segment.Stats()
	if err := c.holder.MarkReplayPlanApplied(plan); err != nil {
		return err
	}
	c.removeOverlayForPlan(plan)
	c.recordSegment(segment, segmentStats)
	if c.segmentHook != nil {
		c.segmentHook(segment, segmentStats)
	}
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

func (c *BufferedCommitter) KeyState(key []byte) (present bool, known bool) {
	if c == nil {
		return false, false
	}
	c.overlayMu.RLock()
	entry, ok := c.overlay[string(key)]
	if ok {
		c.overlayMu.RUnlock()
		return !entry.delete, true
	}
	present, ok = c.known[string(key)]
	c.overlayMu.RUnlock()
	return present, ok
}

func (c *BufferedCommitter) DirectoryEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if c == nil {
		return false
	}
	c.overlayMu.RLock()
	_, ok := c.emptyDirs[DirectoryFactKey(mount, inode)]
	c.overlayMu.RUnlock()
	return ok
}

func (c *BufferedCommitter) RememberKey(key []byte, present bool) {
	if c == nil || len(key) == 0 {
		return
	}
	c.overlayMu.Lock()
	if c.known == nil {
		c.known = make(map[string]bool)
	}
	c.known[string(key)] = present
	c.overlayMu.Unlock()
}

func (c *BufferedCommitter) RememberEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if c == nil {
		return
	}
	c.overlayMu.Lock()
	if c.emptyDirs == nil {
		c.emptyDirs = make(map[string]struct{})
	}
	RememberEmptyDirectoryFact(c.emptyDirs, mount, inode)
	c.overlayMu.Unlock()
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
			"commit_total":                  uint64(0),
			"flush_total":                   uint64(0),
			"apply_total":                   uint64(0),
			"segment_total":                 uint64(0),
			"segment_operations_total":      uint64(0),
			"segment_entries_total":         uint64(0),
			"last_segment_operations":       uint64(0),
			"last_segment_input_mutations":  uint64(0),
			"last_segment_entries":          uint64(0),
			"last_segment_coalesced":        uint64(0),
			"last_segment_compression_x100": uint64(0),
			"last_segment_root":             [32]byte{},
			"error_total":                   uint64(0),
			"overlay_keys":                  0,
			"predicate_known_keys":          0,
			"predicate_empty_dirs":          0,
			"pending":                       0,
		}
	}
	c.overlayMu.RLock()
	overlayKeys := len(c.overlay)
	knownKeys := len(c.known)
	emptyDirs := len(c.emptyDirs)
	c.overlayMu.RUnlock()
	c.statsMu.RLock()
	lastSegmentStats := c.lastSegmentStats
	lastSegmentRoot := c.lastSegmentRoot
	c.statsMu.RUnlock()
	return map[string]any{
		"commit_total":                  c.commitTotal.Load(),
		"flush_total":                   c.flushTotal.Load(),
		"apply_total":                   c.applyTotal.Load(),
		"segment_total":                 c.segmentTotal.Load(),
		"segment_operations_total":      c.segmentOpsTotal.Load(),
		"segment_entries_total":         c.segmentEntryTotal.Load(),
		"last_segment_operations":       lastSegmentStats.OperationCount,
		"last_segment_input_mutations":  lastSegmentStats.InputMutationCount,
		"last_segment_entries":          lastSegmentStats.EntryCount,
		"last_segment_coalesced":        lastSegmentStats.CoalescedMutations,
		"last_segment_compression_x100": uint64(lastSegmentStats.CompressionRatio * 100),
		"last_segment_root":             lastSegmentRoot,
		"error_total":                   c.errorTotal.Load(),
		"overlay_keys":                  overlayKeys,
		"predicate_known_keys":          knownKeys,
		"predicate_empty_dirs":          emptyDirs,
		"pending":                       c.holder.Pending(),
	}
}

func (c *BufferedCommitter) recordSegment(segment PerasSegment, stats SegmentStats) {
	c.segmentTotal.Add(1)
	c.segmentOpsTotal.Add(stats.OperationCount)
	c.segmentEntryTotal.Add(stats.EntryCount)
	c.statsMu.Lock()
	c.lastSegmentStats = stats
	c.lastSegmentRoot = segment.Root
	c.statsMu.Unlock()
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
			return ErrInvalidPerasSegment
		}
		entry := overlayEntry{
			opID: id,
			key:  cloneBytes(effect.Key),
		}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return ErrInvalidPerasSegment
			}
			entry.value = cloneBytes(effect.Value)
		case compile.EffectDelete:
			entry.delete = true
		default:
			return ErrInvalidPerasSegment
		}
		c.overlay[string(effect.Key)] = entry
	}
	if c.known == nil {
		c.known = make(map[string]bool)
	}
	if c.emptyDirs == nil {
		c.emptyDirs = make(map[string]struct{})
	}
	if err := RememberDeltaFacts(c.known, c.emptyDirs, delta); err != nil {
		return err
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
