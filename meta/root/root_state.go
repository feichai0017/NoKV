package root

import "github.com/feichai0017/NoKV/raftstore/descriptor"

// State is the compact checkpointed state of the metadata root.
//
// This is intentionally small. It should only contain globally serialized
// control-plane truth such as allocator fences and topology epochs.
type State struct {
	ClusterEpoch    uint64
	MembershipEpoch uint64
	PolicyVersion   uint64
	LastCommitted   Cursor
	IDFence         uint64
	TSOFence        uint64
}

// Snapshot is the compact materialized rooted metadata state used for bounded
// control-plane bootstrap and recovery.
type Snapshot struct {
	State       State
	Descriptors map[uint64]descriptor.Descriptor
}

// CloneSnapshot returns a detached copy of one rooted metadata snapshot.
func CloneSnapshot(snapshot Snapshot) Snapshot {
	out := Snapshot{
		State:       snapshot.State,
		Descriptors: CloneDescriptors(snapshot.Descriptors),
	}
	return out
}
