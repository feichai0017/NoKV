package lsm

import (
	"github.com/feichai0017/NoKV/index"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
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

type RangeFilterDiagnostics struct {
	PointCandidates   uint64
	PointPruned       uint64
	BoundedCandidates uint64
	BoundedPruned     uint64
	Fallbacks         uint64
}

// Diagnostics exposes a stable read-only snapshot of LSM internals for
// observability code. It keeps runtime metrics grouped behind one API instead
// of leaking internal structures through many top-level getters.
type Diagnostics struct {
	Entries     int64
	Flush       metrics.FlushMetrics
	Compaction  CompactionDiagnostics
	RangeFilter RangeFilterDiagnostics
	Levels      []LevelMetrics
	Cache       CacheMetrics
	MaxVersion  uint64
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
		diag.RangeFilter = lm.rangeFilterDiagnostics()
		diag.Levels = lm.levelMetricsSnapshot()
		diag.Cache = lm.cacheMetrics()
		diag.Entries += lm.entryCount()
	}
	return diag
}

func (lm *levelManager) rangeFilterDiagnostics() RangeFilterDiagnostics {
	if lm == nil {
		return RangeFilterDiagnostics{}
	}
	return RangeFilterDiagnostics{
		PointCandidates:   lm.rangeFilter.pointCandidates.Load(),
		PointPruned:       lm.rangeFilter.pointPruned.Load(),
		BoundedCandidates: lm.rangeFilter.boundedCandidates.Load(),
		BoundedPruned:     lm.rangeFilter.boundedPruned.Load(),
		Fallbacks:         lm.rangeFilter.fallbacks.Load(),
	}
}

// ValueLogHeadSnapshot returns the persisted per-bucket vlog head pointers.
func (lsm *LSM) ValueLogHeadSnapshot() map[uint32]kv.ValuePtr {
	if lsm == nil || lsm.levels == nil {
		return nil
	}
	heads := lsm.levels.ValueLogHead()
	if len(heads) == 0 {
		return nil
	}
	out := make(map[uint32]kv.ValuePtr, len(heads))
	for bucket, meta := range heads {
		if !meta.Valid {
			continue
		}
		out[bucket] = kv.ValuePtr{
			Bucket: bucket,
			Fid:    meta.FileID,
			Offset: uint32(meta.Offset),
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ValueLogStatusSnapshot returns persisted metadata for all known vlog files.
func (lsm *LSM) ValueLogStatusSnapshot() map[manifest.ValueLogID]manifest.ValueLogMeta {
	if lsm == nil || lsm.levels == nil {
		return nil
	}
	return lsm.levels.ValueLogStatus()
}

func countMemIndexEntries(idx memIndex) int64 {
	if idx == nil {
		return 0
	}
	itr := idx.NewIterator(&index.Options{IsAsc: true})
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
