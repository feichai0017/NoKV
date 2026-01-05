package wal

import (
	"log"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
)

// WatchdogConfig controls WAL watchdog behavior.
type WatchdogConfig struct {
	Manager      *Manager
	Interval     time.Duration
	MinRemovable int
	MaxBatch     int
	WarnRatio    float64
	WarnSegments int64
	RaftPointers func() map[uint64]manifest.RaftLogPointer
}

// WatchdogSnapshot captures WAL watchdog state for reporting.
type WatchdogSnapshot struct {
	AutoRuns          uint64
	SegmentsRemoved   uint64
	LastAutoUnix      int64
	LastTickUnix      int64
	RemovableSegments int
	TypedRatio        float64
	Warning           bool
	WarningReason     string
}

// Watchdog periodically inspects WAL backlog and can remove stale segments.
type Watchdog struct {
	manager      *Manager
	interval     time.Duration
	minRemovable int
	maxBatch     int
	warnRatio    float64
	warnSegments int64
	autoEnabled  bool
	raftPointers func() map[uint64]manifest.RaftLogPointer
	closer       *utils.Closer

	autoRuns        atomic.Uint64
	segmentsRemoved atomic.Uint64
	lastAutoUnix    atomic.Int64
	lastTickUnix    atomic.Int64
	removableCount  atomic.Int64
	lastRatioBits   atomic.Uint64
	warnActive      atomic.Bool
	warnReason      atomic.Value
}

// NewWatchdog constructs a watchdog from the provided configuration.
func NewWatchdog(cfg WatchdogConfig) *Watchdog {
	if cfg.Manager == nil {
		return nil
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	minRemovable := cfg.MinRemovable
	if minRemovable <= 0 {
		minRemovable = 1
	}
	maxBatch := cfg.MaxBatch
	if maxBatch <= 0 {
		maxBatch = 4
	}
	w := &Watchdog{
		manager:      cfg.Manager,
		interval:     interval,
		minRemovable: minRemovable,
		maxBatch:     maxBatch,
		warnRatio:    cfg.WarnRatio,
		warnSegments: cfg.WarnSegments,
		autoEnabled:  cfg.MinRemovable > 0 && cfg.MaxBatch > 0,
		raftPointers: cfg.RaftPointers,
		closer:       utils.NewCloser(),
	}
	w.warnReason.Store("")
	return w
}

// Start launches the background watchdog loop.
func (w *Watchdog) Start() {
	if w == nil {
		return
	}
	w.closer.Add(1)
	go w.run()
}

// Stop terminates the background watchdog loop.
func (w *Watchdog) Stop() {
	if w == nil {
		return
	}
	w.closer.Close()
}

// RunOnce executes a single watchdog inspection cycle.
func (w *Watchdog) RunOnce() {
	if w == nil {
		return
	}
	w.observe()
}

// Snapshot returns the current watchdog snapshot.
func (w *Watchdog) Snapshot() WatchdogSnapshot {
	if w == nil {
		return WatchdogSnapshot{}
	}
	ratioBits := w.lastRatioBits.Load()
	snap := WatchdogSnapshot{
		AutoRuns:          w.autoRuns.Load(),
		SegmentsRemoved:   w.segmentsRemoved.Load(),
		LastAutoUnix:      w.lastAutoUnix.Load(),
		LastTickUnix:      w.lastTickUnix.Load(),
		RemovableSegments: int(w.removableCount.Load()),
		TypedRatio:        math.Float64frombits(ratioBits),
		Warning:           w.warnActive.Load(),
	}
	if reason, ok := w.warnReason.Load().(string); ok {
		snap.WarningReason = reason
	}
	return snap
}

func (w *Watchdog) run() {
	defer w.closer.Done()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.observe()

	for {
		select {
		case <-ticker.C:
			w.observe()
		case <-w.closer.CloseSignal:
			return
		}
	}
}

func (w *Watchdog) observe() {
	if w == nil || w.manager == nil {
		return
	}
	w.lastTickUnix.Store(time.Now().Unix())

	wmetrics := w.manager.Metrics()
	segmentMetrics := w.manager.SegmentMetrics()
	var ptrs map[uint64]manifest.RaftLogPointer
	if w.raftPointers != nil {
		ptrs = w.raftPointers()
	}
	analysis := metrics.AnalyzeWALBacklog(wmetrics, segmentMetrics, ptrs)

	w.removableCount.Store(int64(len(analysis.RemovableSegments)))
	w.lastRatioBits.Store(math.Float64bits(analysis.TypedRecordRatio))

	warning, reason := metrics.WALTypedWarning(analysis.TypedRecordRatio, analysis.SegmentsWithRaft, w.warnRatio, w.warnSegments)
	w.warnActive.Store(warning)
	if warning {
		w.warnReason.Store(reason)
	} else {
		w.warnReason.Store("")
	}

	if !w.autoEnabled {
		return
	}
	if len(analysis.RemovableSegments) < w.minRemovable {
		return
	}

	batch := analysis.RemovableSegments
	if len(batch) > w.maxBatch {
		batch = batch[:w.maxBatch]
	}

	removed := 0
	for _, id := range batch {
		if err := w.manager.RemoveSegment(id); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			log.Printf("[wal-watchdog] remove segment %d failed: %v", id, err)
			continue
		}
		removed++
	}
	if removed == 0 {
		return
	}
	w.autoRuns.Add(1)
	w.segmentsRemoved.Add(uint64(removed))
	w.lastAutoUnix.Store(time.Now().Unix())
}
