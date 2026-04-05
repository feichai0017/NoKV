package recovery

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// ReplicaLocalState is the store-local restart state for one hosted replica.
//
// It is not cluster authority. It only exists to make restart recovery and
// local diagnostics explicit.
type ReplicaLocalState struct {
	RegionID    uint64
	LocalPeerID uint64
	State       metaregion.ReplicaState
	Descriptor  *descriptor.Descriptor
	LastApplied uint64
	LastTerm    uint64
}

// RaftProgress is the store-local durable replay progress for one raft group.
type RaftProgress struct {
	GroupID         uint64
	Segment         uint32
	Offset          uint64
	AppliedIndex    uint64
	AppliedTerm     uint64
	Committed       uint64
	SnapshotIndex   uint64
	SnapshotTerm    uint64
	TruncatedIndex  uint64
	TruncatedTerm   uint64
	SegmentIndex    uint64
	TruncatedOffset uint64
}
