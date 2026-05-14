// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package lsm

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/feichai0017/NoKV/metrics"
)

// CompactionDiagnostics groups compaction runtime counters for diagnostics
// and observability consumers.
type CompactionDiagnostics struct {
	Backlog        int64
	MaxScore       float64
	LastDurationMs float64
	MaxDurationMs  float64
	Runs           uint64
	ValueWeight    float64
	AlertThreshold float64
}

// RangeFilterDiagnostics groups range-filter hit/miss counters.
type RangeFilterDiagnostics struct {
	PointCandidates   uint64
	PointPruned       uint64
	BoundedCandidates uint64
	BoundedPruned     uint64
	Fallbacks         uint64
}

// Diagnostics is a stable read-only snapshot of LSM internals for
// observability code. It keeps runtime metrics grouped behind one API
// instead of leaking internal structures through many top-level getters.
type Diagnostics struct {
	Entries     int64
	Flush       metrics.FlushMetrics
	Compaction  CompactionDiagnostics
	RangeFilter RangeFilterDiagnostics
	Levels      []metrics.LevelMetrics
	Cache       metrics.CacheSnapshot
	MaxVersion  uint64
}

// Diagnostics returns a point-in-time snapshot of LSM diagnostic state.
func (lsm *LSM) Diagnostics() Diagnostics {
	if lsm == nil {
		return Diagnostics{}
	}
	diag := Diagnostics{
		MaxVersion: lsm.maxVersion(),
	}
	if view := lsm.getMemTables(); view != nil {
		defer view.DecrRef()
		for _, mt := range view.Tables() {
			if mt == nil || mt.index == nil {
				continue
			}
			diag.Entries += countMemIndexEntries(mt.index)
		}
	}
	if lsm.flushPool != nil {
		diag.Flush = lsm.flushPool.Stats()
	}
	if lsm.option != nil {
		diag.Compaction.ValueWeight = lsm.option.CompactionValueWeight
		diag.Compaction.AlertThreshold = lsm.option.CompactionValueAlertThreshold
	}
	if lm := lsm.levels; lm != nil {
		diag.Compaction.Backlog, diag.Compaction.MaxScore = lm.compactor.priorityStats()
		diag.Compaction.LastDurationMs, diag.Compaction.MaxDurationMs, diag.Compaction.Runs = lm.compactor.runDurations()
		diag.RangeFilter = lm.rangeFilterDiagnostics()
		diag.Levels = lm.levelMetricsSnapshot()
		diag.Cache = lm.cacheMetrics()
		diag.Entries += lm.entryCount()
	}
	return diag
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

// rangeFilterMetrics is the in-memory hit/miss counter set used to populate
// RangeFilterDiagnostics.
type rangeFilterMetrics struct {
	pointCandidates   atomic.Uint64
	pointPruned       atomic.Uint64
	boundedCandidates atomic.Uint64
	boundedPruned     atomic.Uint64
	fallbacks         atomic.Uint64
}

func (lm *levelManager) recordRangeFilterPoint(total, candidates int, fallback bool) {
	if lm == nil {
		return
	}
	if candidates < 0 {
		candidates = 0
	}
	if total < candidates {
		total = candidates
	}
	lm.rangeFilter.pointCandidates.Add(uint64(candidates))
	lm.rangeFilter.pointPruned.Add(uint64(total - candidates))
	if fallback {
		lm.rangeFilter.fallbacks.Add(1)
	}
}

func (lm *levelManager) recordRangeFilterBounded(total, candidates int, fallback bool) {
	if lm == nil {
		return
	}
	if candidates < 0 {
		candidates = 0
	}
	if total < candidates {
		total = candidates
	}
	lm.rangeFilter.boundedCandidates.Add(uint64(candidates))
	lm.rangeFilter.boundedPruned.Add(uint64(total - candidates))
	if fallback {
		lm.rangeFilter.fallbacks.Add(1)
	}
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

func (lh *levelHandler) tablesSnapshot() []*table.Table {
	if lh == nil {
		return nil
	}
	lh.RLock()
	defer lh.RUnlock()
	out := make([]*table.Table, len(lh.tables))
	copy(out, lh.tables)
	return out
}
