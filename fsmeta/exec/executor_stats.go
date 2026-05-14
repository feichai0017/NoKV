// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type perasAdmissionCounters struct {
	eligibleTotal         atomic.Uint64
	slowTotal             atomic.Uint64
	slowReadOnlyTotal     atomic.Uint64
	slowRangeReadTotal    atomic.Uint64
	slowDurabilityTotal   atomic.Uint64
	slowCrossBucketTotal  atomic.Uint64
	slowSharedQuotaTotal  atomic.Uint64
	slowDynamicWriteTotal atomic.Uint64
	slowMaintenanceTotal  atomic.Uint64
	slowUnknownTotal      atomic.Uint64
	acquireTotal          atomic.Uint64
	ownedTotal            atomic.Uint64
	heldTotal             atomic.Uint64
	errorTotal            atomic.Uint64
}

type perasVisibleCounters struct {
	attemptTotal           atomic.Uint64
	successTotal           atomic.Uint64
	errorTotal             atomic.Uint64
	skipIneligibleTotal    atomic.Uint64
	skipNoAuthorityTotal   atomic.Uint64
	skipNonConcreteTotal   atomic.Uint64
	skipPlacementTotal     atomic.Uint64
	skipPredicateTotal     atomic.Uint64
	latencyTotalNanosecond atomic.Uint64
	latencyMaxNanosecond   atomic.Uint64
}

type perasDirectoryReadCounters struct {
	total      atomic.Uint64
	perasOnly  atomic.Uint64
	dirIndex   atomic.Uint64
	baseRows   atomic.Uint64
	perasRows  atomic.Uint64
	outputRows atomic.Uint64
}

type atomicOnePhaseCounters struct {
	attemptTotal           atomic.Uint64
	skipTotal              atomic.Uint64
	backoffSkipTotal       atomic.Uint64
	runnerUnsupportedTotal atomic.Uint64
	fallbackTotal          atomic.Uint64
	successTotal           atomic.Uint64
	consecutiveFallbacks   atomic.Uint64
	mu                     sync.Mutex
	fallbacksByAffinity    map[string]uint64
}

// Stats returns executor counters suitable for expvar export.
func (e *Executor) Stats() map[string]any {
	if e == nil {
		return map[string]any{
			"read_retries_total":         uint64(0),
			"read_retry_exhausted_total": uint64(0),
			"txn_retries_total":          uint64(0),
			"txn_retry_exhausted_total":  uint64(0),
			"create_total":               uint64(0),
			"commit_contract":            commitContractStats(false),
			"peras_admission":            perasAdmissionStats(nil, false),
			"peras_visible_commit":       perasVisibleStats(nil, false),
			"peras_directory_read":       perasDirectoryReadStats(nil),
			"atomic_one_phase":           atomicOnePhaseStats(nil),
			"negative_cache_enabled":     false,
			"dirpage_cache_enabled":      false,
		}
	}
	out := map[string]any{
		"read_retries_total":         e.readRetriesTotal.Load(),
		"read_retry_exhausted_total": e.readRetryExhaustedTotal.Load(),
		"txn_retries_total":          e.txnRetriesTotal.Load(),
		"txn_retry_exhausted_total":  e.txnRetryExhaustedTotal.Load(),
		"create_total":               e.createTotal.Load(),
		"commit_contract":            commitContractStats(e.perasCommitter != nil),
		"peras_admission":            perasAdmissionStats(&e.perasAdmission, e.perasAuthority != nil),
		"peras_visible_commit":       perasVisibleStats(&e.perasVisible, e.perasCommitter != nil),
		"peras_directory_read":       perasDirectoryReadStats(&e.perasDirectoryRead),
		"atomic_one_phase":           atomicOnePhaseStats(e.atomicOnePhase),
		"negative_cache_enabled":     e.negCache != nil,
		"dirpage_cache_enabled":      e.dirPages != nil,
	}
	if stats, ok := e.runner.(statsProvider); ok {
		out["runner"] = stats.Stats()
	}
	if stats, ok := e.perasCommitter.(statsProvider); ok {
		out["peras_committer"] = stats.Stats()
	}
	if e.dirPages != nil {
		stats := e.dirPages.Stats()
		out["dirpage_hits"] = stats.Hits
		out["dirpage_misses"] = stats.Misses
		out["dirpage_stale"] = stats.Stale
		out["dirpage_store_ok"] = stats.StoreOK
		out["dirpage_dropped"] = stats.Dropped
	}
	if stats, ok := e.inodes.(statsProvider); ok {
		out["inode_allocator"] = stats.Stats()
	}
	return out
}

func perasDirectoryReadStats(counters *perasDirectoryReadCounters) map[string]any {
	if counters == nil {
		return map[string]any{
			"total":       uint64(0),
			"peras_only":  uint64(0),
			"dir_index":   uint64(0),
			"base_rows":   uint64(0),
			"peras_rows":  uint64(0),
			"output_rows": uint64(0),
		}
	}
	return map[string]any{
		"total":       counters.total.Load(),
		"peras_only":  counters.perasOnly.Load(),
		"dir_index":   counters.dirIndex.Load(),
		"base_rows":   counters.baseRows.Load(),
		"peras_rows":  counters.perasRows.Load(),
		"output_rows": counters.outputRows.Load(),
	}
}

func commitContractStats(perasEnabled bool) map[string]any {
	if perasEnabled {
		return map[string]any{
			"default_write_path":        "peras",
			"successful_write_boundary": "visible",
			"durable_boundary":          "witness_quorum_plus_raftstore_segment_install",
		}
	}
	return map[string]any{
		"default_write_path":        "percolator",
		"successful_write_boundary": "durable",
		"durable_boundary":          "raftstore_commit",
	}
}

func (s *perasDirectoryReadCounters) record(stats compile.DirectoryReadStats) {
	s.total.Add(1)
	if stats.UsedPerasOnly {
		s.perasOnly.Add(1)
	}
	if stats.UsedDirIndex {
		s.dirIndex.Add(1)
	}
	s.baseRows.Add(uint64(stats.BaseRows))
	s.perasRows.Add(uint64(stats.PerasRows))
	s.outputRows.Add(uint64(stats.OutputRows))
}

func perasAdmissionStats(counters *perasAdmissionCounters, enabled bool) map[string]any {
	if counters == nil {
		return map[string]any{
			"enabled":        enabled,
			"eligible_total": uint64(0),
			"slow_total":     uint64(0),
			"slow_by_reason": perasAdmissionSlowReasonStats(nil),
			"acquire_total":  uint64(0),
			"owned_total":    uint64(0),
			"held_total":     uint64(0),
			"error_total":    uint64(0),
		}
	}
	return map[string]any{
		"enabled":        enabled,
		"eligible_total": counters.eligibleTotal.Load(),
		"slow_total":     counters.slowTotal.Load(),
		"slow_by_reason": perasAdmissionSlowReasonStats(counters),
		"acquire_total":  counters.acquireTotal.Load(),
		"owned_total":    counters.ownedTotal.Load(),
		"held_total":     counters.heldTotal.Load(),
		"error_total":    counters.errorTotal.Load(),
	}
}

func perasVisibleStats(counters *perasVisibleCounters, enabled bool) map[string]any {
	if counters == nil {
		return map[string]any{
			"enabled":                    enabled,
			"attempt_total":              uint64(0),
			"success_total":              uint64(0),
			"error_total":                uint64(0),
			"skip_ineligible_total":      uint64(0),
			"skip_no_authority_total":    uint64(0),
			"skip_non_concrete_total":    uint64(0),
			"skip_placement_total":       uint64(0),
			"skip_predicate_total":       uint64(0),
			"latency_total_nanosecond":   uint64(0),
			"latency_max_nanosecond":     uint64(0),
			"latency_average_nanosecond": uint64(0),
		}
	}
	attempts := counters.attemptTotal.Load()
	latency := counters.latencyTotalNanosecond.Load()
	average := uint64(0)
	if attempts > 0 {
		average = latency / attempts
	}
	return map[string]any{
		"enabled":                    enabled,
		"attempt_total":              attempts,
		"success_total":              counters.successTotal.Load(),
		"error_total":                counters.errorTotal.Load(),
		"skip_ineligible_total":      counters.skipIneligibleTotal.Load(),
		"skip_no_authority_total":    counters.skipNoAuthorityTotal.Load(),
		"skip_non_concrete_total":    counters.skipNonConcreteTotal.Load(),
		"skip_placement_total":       counters.skipPlacementTotal.Load(),
		"skip_predicate_total":       counters.skipPredicateTotal.Load(),
		"latency_total_nanosecond":   latency,
		"latency_max_nanosecond":     counters.latencyMaxNanosecond.Load(),
		"latency_average_nanosecond": average,
	}
}

func recordUint64Max(max *atomic.Uint64, value uint64) {
	if max == nil {
		return
	}
	for {
		old := max.Load()
		if value <= old {
			return
		}
		if max.CompareAndSwap(old, value) {
			return
		}
	}
}

func perasAdmissionSlowReasonStats(counters *perasAdmissionCounters) map[string]uint64 {
	if counters == nil {
		return map[string]uint64{
			string(compile.SlowReasonReadOnly):          0,
			string(compile.SlowReasonRangeRead):         0,
			string(compile.SlowReasonDurabilityBarrier): 0,
			string(compile.SlowReasonCrossBucket):       0,
			string(compile.SlowReasonSharedQuota):       0,
			string(compile.SlowReasonDynamicWriteSet):   0,
			string(compile.SlowReasonMaintenanceScan):   0,
			"unknown": 0,
		}
	}
	return map[string]uint64{
		string(compile.SlowReasonReadOnly):          counters.slowReadOnlyTotal.Load(),
		string(compile.SlowReasonRangeRead):         counters.slowRangeReadTotal.Load(),
		string(compile.SlowReasonDurabilityBarrier): counters.slowDurabilityTotal.Load(),
		string(compile.SlowReasonCrossBucket):       counters.slowCrossBucketTotal.Load(),
		string(compile.SlowReasonSharedQuota):       counters.slowSharedQuotaTotal.Load(),
		string(compile.SlowReasonDynamicWriteSet):   counters.slowDynamicWriteTotal.Load(),
		string(compile.SlowReasonMaintenanceScan):   counters.slowMaintenanceTotal.Load(),
		"unknown": counters.slowUnknownTotal.Load(),
	}
}

func (s *perasAdmissionCounters) recordSlow(reason compile.SlowReason) {
	s.slowTotal.Add(1)
	switch reason {
	case compile.SlowReasonReadOnly:
		s.slowReadOnlyTotal.Add(1)
	case compile.SlowReasonRangeRead:
		s.slowRangeReadTotal.Add(1)
	case compile.SlowReasonDurabilityBarrier:
		s.slowDurabilityTotal.Add(1)
	case compile.SlowReasonCrossBucket:
		s.slowCrossBucketTotal.Add(1)
	case compile.SlowReasonSharedQuota:
		s.slowSharedQuotaTotal.Add(1)
	case compile.SlowReasonDynamicWriteSet:
		s.slowDynamicWriteTotal.Add(1)
	case compile.SlowReasonMaintenanceScan:
		s.slowMaintenanceTotal.Add(1)
	default:
		s.slowUnknownTotal.Add(1)
	}
}

var atomicOnePhaseKinds = [...]fsmeta.OperationKind{
	fsmeta.OperationCreate,
	fsmeta.OperationUpdateInode,
	fsmeta.OperationRename,
	fsmeta.OperationLink,
	fsmeta.OperationUnlink,
	fsmeta.OperationOpenWriteSession,
	fsmeta.OperationHeartbeatSession,
	fsmeta.OperationCloseSession,
}

func newAtomicOnePhaseCounters() map[fsmeta.OperationKind]*atomicOnePhaseCounters {
	out := make(map[fsmeta.OperationKind]*atomicOnePhaseCounters, len(atomicOnePhaseKinds))
	for _, kind := range atomicOnePhaseKinds {
		out[kind] = &atomicOnePhaseCounters{fallbacksByAffinity: make(map[string]uint64)}
	}
	return out
}

func atomicOnePhaseStats(counters map[fsmeta.OperationKind]*atomicOnePhaseCounters) map[string]any {
	out := make(map[string]any, len(atomicOnePhaseKinds))
	for _, kind := range atomicOnePhaseKinds {
		var stats *atomicOnePhaseCounters
		if counters != nil {
			stats = counters[kind]
		}
		out[string(kind)] = atomicOnePhaseStatsFor(stats)
	}
	return out
}

func atomicOnePhaseStatsFor(stats *atomicOnePhaseCounters) map[string]uint64 {
	if stats == nil {
		return map[string]uint64{
			"attempt_total":            0,
			"skip_total":               0,
			"backoff_skip_total":       0,
			"runner_unsupported_total": 0,
			"fallback_total":           0,
			"success_total":            0,
			"consecutive_fallbacks":    0,
		}
	}
	return map[string]uint64{
		"attempt_total":            stats.attemptTotal.Load(),
		"skip_total":               stats.skipTotal.Load(),
		"backoff_skip_total":       stats.backoffSkipTotal.Load(),
		"runner_unsupported_total": stats.runnerUnsupportedTotal.Load(),
		"fallback_total":           stats.fallbackTotal.Load(),
		"success_total":            stats.successTotal.Load(),
		"consecutive_fallbacks":    stats.consecutiveFallbacks.Load(),
	}
}
