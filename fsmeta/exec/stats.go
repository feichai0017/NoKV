// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

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
			"commit_contract":              commitContractStats(),
			"metadata_predicate_commit":    metadataPredicateStats(nil),
		}
	}
	out := map[string]any{
		"read_retries_total":           e.readRetriesTotal.Load(),
		"read_retry_exhausted_total":   e.readRetryExhaustedTotal.Load(),
		"commit_retries_total":         e.commitRetriesTotal.Load(),
		"commit_retry_exhausted_total": e.commitRetryExhaustedTotal.Load(),
		"create_total":                 e.createTotal.Load(),
		"commit_contract":              commitContractStats(),
		"metadata_predicate_commit":    metadataPredicateStats(e.metadataPredicates),
	}
	if stats, ok := e.runner.(backend.StatsProvider); ok {
		out["runner"] = stats.Stats()
	}
	if stats, ok := e.inodes.(backend.StatsProvider); ok {
		out["inode_allocator"] = stats.Stats()
	}
	return out
}

func commitContractStats() map[string]any {
	return map[string]any{
		"default_write_path":        "backend_commit",
		"successful_write_boundary": "durable",
		"durable_boundary":          "backend_commit",
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
