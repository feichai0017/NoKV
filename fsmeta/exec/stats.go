// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type visibleAdmissionCounters struct {
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

type visibleCommitCounters struct {
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

type visibleDirectoryReadCounters struct {
	total       atomic.Uint64
	visibleOnly atomic.Uint64
	dirIndex    atomic.Uint64
	baseRows    atomic.Uint64
	visibleRows atomic.Uint64
	outputRows  atomic.Uint64
}

type metadataPredicateCounters struct {
	attemptTotal atomic.Uint64
	skipTotal    atomic.Uint64
	successTotal atomic.Uint64
}

// Stats returns executor counters suitable for expvar export.
func (e *Executor) Stats() map[string]any {
	if e == nil {
		return map[string]any{
			"read_retries_total":           uint64(0),
			"read_retry_exhausted_total":   uint64(0),
			"commit_retries_total":         uint64(0),
			"commit_retry_exhausted_total": uint64(0),
			"create_total":                 uint64(0),
			"commit_contract":              commitContractStats(false),
			"visible_admission":            visibleAdmissionStats(nil, false),
			"visible_commit":               visibleCommitStats(nil, false),
			"visible_directory_read":       visibleDirectoryReadStats(nil),
			"metadata_predicate_commit":    metadataPredicateStats(nil),
		}
	}
	out := map[string]any{
		"read_retries_total":           e.readRetriesTotal.Load(),
		"read_retry_exhausted_total":   e.readRetryExhaustedTotal.Load(),
		"commit_retries_total":         e.commitRetriesTotal.Load(),
		"commit_retry_exhausted_total": e.commitRetryExhaustedTotal.Load(),
		"create_total":                 e.createTotal.Load(),
		"commit_contract":              commitContractStats(e.visibleCommitter != nil),
		"visible_admission":            visibleAdmissionStats(&e.visibleAdmission, e.visibleAuthority != nil),
		"visible_commit":               visibleCommitStats(&e.visibleCommit, e.visibleCommitter != nil),
		"visible_directory_read":       visibleDirectoryReadStats(&e.visibleDirectoryRead),
		"metadata_predicate_commit":    metadataPredicateStats(e.metadataPredicates),
	}
	if stats, ok := e.runner.(backend.StatsProvider); ok {
		out["runner"] = stats.Stats()
	}
	if stats, ok := e.visibleCommitter.(backend.StatsProvider); ok {
		out["visible_committer"] = stats.Stats()
	}
	if stats, ok := e.inodes.(backend.StatsProvider); ok {
		out["inode_allocator"] = stats.Stats()
	}
	return out
}

func visibleDirectoryReadStats(counters *visibleDirectoryReadCounters) map[string]any {
	if counters == nil {
		return map[string]any{
			"total":        uint64(0),
			"visible_only": uint64(0),
			"dir_index":    uint64(0),
			"base_rows":    uint64(0),
			"visible_rows": uint64(0),
			"output_rows":  uint64(0),
		}
	}
	return map[string]any{
		"total":        counters.total.Load(),
		"visible_only": counters.visibleOnly.Load(),
		"dir_index":    counters.dirIndex.Load(),
		"base_rows":    counters.baseRows.Load(),
		"visible_rows": counters.visibleRows.Load(),
		"output_rows":  counters.outputRows.Load(),
	}
}

func commitContractStats(visibleEnabled bool) map[string]any {
	if visibleEnabled {
		return map[string]any{
			"default_write_path":        "visible_commit",
			"successful_write_boundary": "visible",
			"durable_boundary":          "witness_quorum_plus_raftstore_segment_install",
		}
	}
	return map[string]any{
		"default_write_path":        "backend_commit",
		"successful_write_boundary": "durable",
		"durable_boundary":          "backend_commit",
	}
}

func (s *visibleDirectoryReadCounters) record(stats compile.DirectoryReadStats) {
	s.total.Add(1)
	if stats.UsedOverlayOnly {
		s.visibleOnly.Add(1)
	}
	if stats.UsedDirIndex {
		s.dirIndex.Add(1)
	}
	s.baseRows.Add(uint64(stats.BaseRows))
	s.visibleRows.Add(uint64(stats.OverlayRows))
	s.outputRows.Add(uint64(stats.OutputRows))
}

func visibleAdmissionStats(counters *visibleAdmissionCounters, enabled bool) map[string]any {
	if counters == nil {
		return map[string]any{
			"enabled":        enabled,
			"eligible_total": uint64(0),
			"slow_total":     uint64(0),
			"slow_by_reason": visibleAdmissionSlowReasonStats(nil),
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
		"slow_by_reason": visibleAdmissionSlowReasonStats(counters),
		"acquire_total":  counters.acquireTotal.Load(),
		"owned_total":    counters.ownedTotal.Load(),
		"held_total":     counters.heldTotal.Load(),
		"error_total":    counters.errorTotal.Load(),
	}
}

func visibleCommitStats(counters *visibleCommitCounters, enabled bool) map[string]any {
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

func visibleAdmissionSlowReasonStats(counters *visibleAdmissionCounters) map[string]uint64 {
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

var metadataPredicateKinds = [...]model.OperationKind{
	model.OperationCreate,
	model.OperationUpdateInode,
	model.OperationRename,
	model.OperationLink,
	model.OperationUnlink,
	model.OperationRemove,
	model.OperationOpenWriteSession,
	model.OperationHeartbeatSession,
	model.OperationCloseSession,
}

func newMetadataPredicateCounters() map[model.OperationKind]*metadataPredicateCounters {
	out := make(map[model.OperationKind]*metadataPredicateCounters, len(metadataPredicateKinds))
	for _, kind := range metadataPredicateKinds {
		out[kind] = &metadataPredicateCounters{}
	}
	return out
}

func metadataPredicateStats(counters map[model.OperationKind]*metadataPredicateCounters) map[string]any {
	out := make(map[string]any, len(metadataPredicateKinds))
	for _, kind := range metadataPredicateKinds {
		var stats *metadataPredicateCounters
		if counters != nil {
			stats = counters[kind]
		}
		out[string(kind)] = metadataPredicateStatsFor(stats)
	}
	return out
}

func metadataPredicateStatsFor(stats *metadataPredicateCounters) map[string]uint64 {
	if stats == nil {
		return map[string]uint64{
			"attempt_total": 0,
			"skip_total":    0,
			"success_total": 0,
		}
	}
	return map[string]uint64{
		"attempt_total": stats.attemptTotal.Load(),
		"skip_total":    stats.skipTotal.Load(),
		"success_total": stats.successTotal.Load(),
	}
}
