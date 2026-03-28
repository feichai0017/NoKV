package NoKV

import (
	"log/slog"

	"github.com/feichai0017/NoKV/lsm"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/wal"
)

// dbWALGCPolicy adapts DB-level raft/WAL metadata into the LSM WAL GC-policy
// interface.
//
// Design notes:
//   - LSM calls CanRemoveSegment(segmentID) and does not depend on raft-specific types.
//   - DB provides raft pointer snapshots and per-segment WAL metrics.
//   - The policy preserves existing semantics: retain segments still referenced
//     by raft pointers and warn when a removable segment still contains raft
//     records.
type dbWALGCPolicy struct {
	raftPointers   func() map[uint64]raftmeta.RaftLogPointer
	segmentMetrics func(segmentID uint32) wal.RecordMetrics
	warn           func(msg string, args ...any)
}

// newDBWALGCPolicy builds the default DB-backed WAL GC policy.
//
// The adapter reads raft checkpoints from the top-level DB options callback and
// record counters from the DB WAL manager. It is injected into LSM so LSM core stays
// decoupled from store-local raft metadata and WAL typed-record details.
func newDBWALGCPolicy(db *DB) lsm.WALGCPolicy {
	if db == nil {
		return &dbWALGCPolicy{}
	}
	w := db.wal
	return &dbWALGCPolicy{
		raftPointers: db.opt.RaftPointerSnapshot,
		segmentMetrics: func(segmentID uint32) wal.RecordMetrics {
			if w == nil {
				return wal.RecordMetrics{}
			}
			return w.SegmentRecordMetrics(segmentID)
		},
		warn: func(msg string, args ...any) {
			slog.Default().Warn(msg, args...)
		},
	}
}

// CanRemoveSegment reports whether the target WAL segment can be garbage-collected.
//
// A segment is retained when any raft pointer still references that segment or
// a later one. When the segment is otherwise removable but still contains raft
// record types, the policy emits a warning for observability and still returns
// true to preserve existing cleanup behavior.
func (p *dbWALGCPolicy) CanRemoveSegment(segmentID uint32) bool {
	if p == nil {
		return true
	}
	if p.raftPointers != nil {
		ptrs := p.raftPointers()
		for _, ptr := range ptrs {
			if ptr.SegmentIndex > 0 && segmentID >= uint32(ptr.SegmentIndex) {
				return false
			}
			if ptr.Segment > 0 && segmentID >= ptr.Segment {
				return false
			}
		}
	}
	if p.segmentMetrics != nil {
		metrics := p.segmentMetrics(segmentID)
		if metrics.RaftRecords() > 0 && p.warn != nil {
			p.warn(
				"wal segment retains raft records during GC eligibility",
				"segment", segmentID,
				"raft_entries", metrics.RaftEntries,
				"raft_states", metrics.RaftStates,
				"raft_snapshots", metrics.RaftSnapshots,
			)
		}
	}
	return true
}
