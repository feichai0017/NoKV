// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package lsm

import "time"

// recordRun updates compaction-run latency counters. Lives here so all LSM
// metric write paths are co-located with the recordX helpers per
// code_contract §9.
func (c *compactor) recordRun(duration time.Duration) {
	c.metrics.Runs.Add(1)
	last := duration.Nanoseconds()
	c.metrics.LastNs.Store(last)
	for {
		prev := c.metrics.MaxNs.Load()
		if last <= prev {
			break
		}
		if c.metrics.MaxNs.CompareAndSwap(prev, last) {
			break
		}
	}
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

func (lh *levelHandler) recordLandingMetrics(merge bool, duration time.Duration, tables int) {
	if tables < 0 {
		tables = 0
	}
	if merge {
		lh.landingMergeRuns.Add(1)
		lh.landingMergeDurationNs.Add(duration.Nanoseconds())
		if tables > 0 {
			lh.landingMergeTables.Add(uint64(tables))
		}
		return
	}
	lh.landingRuns.Add(1)
	lh.landingDurationNs.Add(duration.Nanoseconds())
	if tables > 0 {
		lh.landingTablesCompactedCount.Add(uint64(tables))
	}
}
