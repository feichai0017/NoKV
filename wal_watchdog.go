package NoKV

import (
	"log"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/internal/metrics"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
)

type walWatchdog struct {
	db           *DB
	interval     time.Duration
	minRemovable int
	maxBatch     int
	warnRatio    float64
	warnSegments int64
	autoEnabled  bool
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

type walWatchdogSnapshot struct {
	AutoRuns          uint64
	SegmentsRemoved   uint64
	LastAutoUnix      int64
	LastTickUnix      int64
	RemovableSegments int
	TypedRatio        float64
	Warning           bool
	WarningReason     string
}

func newWalWatchdog(db *DB) *walWatchdog {
	if db == nil || db.opt == nil {
		return nil
	}
	cfg := db.opt
	if !cfg.EnableWALWatchdog {
		return nil
	}
	interval := cfg.WALAutoGCInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	minRemovable := cfg.WALAutoGCMinRemovable
	if minRemovable <= 0 {
		minRemovable = 1
	}
	maxBatch := cfg.WALAutoGCMaxBatch
	if maxBatch <= 0 {
		maxBatch = 4
	}
	watcher := &walWatchdog{
		db:           db,
		interval:     interval,
		minRemovable: minRemovable,
		maxBatch:     maxBatch,
		warnRatio:    cfg.WALTypedRecordWarnRatio,
		warnSegments: cfg.WALTypedRecordWarnSegments,
		autoEnabled:  cfg.WALAutoGCMinRemovable > 0 && cfg.WALAutoGCMaxBatch > 0,
		closer:       utils.NewCloser(),
	}
	watcher.warnReason.Store("")
	return watcher
}

func (w *walWatchdog) start() {
	if w == nil {
		return
	}
	w.closer.Add(1)
	go w.run()
}

func (w *walWatchdog) stop() {
	if w == nil {
		return
	}
	w.closer.Close()
}

func (w *walWatchdog) runOnce() {
	if w == nil {
		return
	}
	w.observe()
}

func (w *walWatchdog) snapshot() walWatchdogSnapshot {
	if w == nil {
		return walWatchdogSnapshot{}
	}
	ratioBits := w.lastRatioBits.Load()
	snap := walWatchdogSnapshot{
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

func (w *walWatchdog) run() {
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

func (w *walWatchdog) observe() {
	if w == nil || w.db == nil || w.db.wal == nil {
		return
	}
	w.lastTickUnix.Store(time.Now().Unix())

	wmetrics := w.db.wal.Metrics()
	segmentMetrics := w.db.wal.SegmentMetrics()
	var ptrs map[uint64]manifest.RaftLogPointer
	if man := w.db.Manifest(); man != nil {
		ptrs = man.RaftPointerSnapshot()
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
		if err := w.db.wal.RemoveSegment(id); err != nil {
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
