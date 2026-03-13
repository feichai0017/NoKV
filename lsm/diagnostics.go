package lsm

import (
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
)

// CompactionDiagnostics groups compaction runtime counters for diagnostics and
// observability consumers. It is intentionally a snapshot view, not a control
// surface.
type CompactionDiagnostics struct {
	Backlog        int64
	MaxScore       float64
	LastDurationMs float64
	MaxDurationMs  float64
	Runs           uint64
	ValueWeight    float64
	AlertThreshold float64
}

// Diagnostics exposes a stable read-only snapshot of LSM internals for
// observability code. It keeps runtime metrics grouped behind one API instead
// of leaking internal structures through many top-level getters.
type Diagnostics struct {
	Entries        int64
	Flush          metrics.FlushMetrics
	Compaction     CompactionDiagnostics
	Levels         []LevelMetrics
	Cache          CacheMetrics
	ValueLogHead   map[uint32]kv.ValuePtr
	ValueLogStatus map[manifest.ValueLogID]manifest.ValueLogMeta
	CurrentVersion manifest.Version
	MaxVersion     uint64
}

// Diagnostics returns a point-in-time snapshot of LSM diagnostic state.
func (lsm *LSM) Diagnostics() Diagnostics {
	if lsm == nil {
		return Diagnostics{}
	}
	diag := Diagnostics{
		MaxVersion: lsm.MaxVersion(),
	}
	if tables, release := lsm.getMemTables(); tables != nil {
		if release != nil {
			defer release()
		}
		for _, mt := range tables {
			if mt == nil || mt.index == nil {
				continue
			}
			diag.Entries += countMemIndexEntries(mt.index)
		}
	}
	if lsm.flushQueue != nil {
		diag.Flush = lsm.flushQueue.stats()
	}
	if lsm.option != nil {
		diag.Compaction.ValueWeight = lsm.option.CompactionValueWeight
		diag.Compaction.AlertThreshold = lsm.option.CompactionValueAlertThreshold
	}
	if lm := lsm.levels; lm != nil {
		diag.Compaction.Backlog, diag.Compaction.MaxScore = lm.compactionStats()
		diag.Compaction.LastDurationMs, diag.Compaction.MaxDurationMs, diag.Compaction.Runs = lm.compactionDurations()
		diag.Levels = lm.levelMetricsSnapshot()
		diag.Cache = lm.cacheMetrics()
		diag.Entries += lm.entryCount()
		if heads := lm.ValueLogHead(); len(heads) > 0 {
			diag.ValueLogHead = make(map[uint32]kv.ValuePtr, len(heads))
			for bucket, meta := range heads {
				if !meta.Valid {
					continue
				}
				diag.ValueLogHead[bucket] = kv.ValuePtr{
					Bucket: bucket,
					Fid:    meta.FileID,
					Offset: uint32(meta.Offset),
				}
			}
			if len(diag.ValueLogHead) == 0 {
				diag.ValueLogHead = nil
			}
		}
		if status := lm.ValueLogStatus(); len(status) > 0 {
			diag.ValueLogStatus = status
		}
		if lm.manifestMgr != nil {
			diag.CurrentVersion = lm.manifestMgr.Current()
		}
	}
	return diag
}

func countMemIndexEntries(idx memIndex) int64 {
	if idx == nil {
		return 0
	}
	itr := idx.NewIterator(&utils.Options{IsAsc: true})
	if itr == nil {
		return 0
	}
	defer func() { _ = itr.Close() }()
	itr.Rewind()
	var count int64
	for ; itr.Valid(); itr.Next() {
		count++
	}
	return count
}

func (lm *levelManager) entryCount() int64 {
	if lm == nil {
		return 0
	}
	var total int64
	for _, level := range lm.levels {
		if level == nil {
			continue
		}
		for _, tbl := range level.tablesSnapshot() {
			if tbl == nil {
				continue
			}
			total += int64(tbl.KeyCount())
		}
	}
	return total
}

func (lh *levelHandler) tablesSnapshot() []*table {
	if lh == nil {
		return nil
	}
	lh.RLock()
	defer lh.RUnlock()
	out := make([]*table, len(lh.tables))
	copy(out, lh.tables)
	return out
}
