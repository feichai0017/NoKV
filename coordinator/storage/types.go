package storage

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// AllocatorState captures persisted counters for ID and TSO allocators.
type AllocatorState struct {
	IDCurrent uint64
	TSCurrent uint64
}

type CatchUpState uint8

const (
	CatchUpStateUnspecified CatchUpState = iota
	CatchUpStateFresh
	CatchUpStateLagging
	CatchUpStateBootstrapRequired
	CatchUpStateUnavailable
)

// Snapshot is the reconstructed Coordinator bootstrap catalog derived from durable
// metadata-root truth.
type Snapshot struct {
	ClusterEpoch        uint64
	RootToken           rootstorage.TailToken
	CatchUpState        CatchUpState
	Descriptors         map[uint64]descriptor.Descriptor
	PendingPeerChanges  map[uint64]rootstate.PendingPeerChange
	PendingRangeChanges map[uint64]rootstate.PendingRangeChange
	Allocator           AllocatorState
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	return Snapshot{
		ClusterEpoch:        snapshot.ClusterEpoch,
		RootToken:           snapshot.RootToken,
		CatchUpState:        snapshot.CatchUpState,
		Descriptors:         rootstate.CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(snapshot.PendingRangeChanges),
		Allocator:           snapshot.Allocator,
	}
}

func SnapshotFromRoot(snapshot rootstate.Snapshot) Snapshot {
	return Snapshot{
		ClusterEpoch: snapshot.State.ClusterEpoch,
		RootToken: rootstorage.TailToken{
			Cursor:   snapshot.State.LastCommitted,
			Revision: 0,
		},
		CatchUpState:        CatchUpStateFresh,
		Descriptors:         rootstate.CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(snapshot.PendingRangeChanges),
		Allocator: AllocatorState{
			IDCurrent: snapshot.State.IDFence,
			TSCurrent: snapshot.State.TSOFence,
		},
	}
}

// BootstrapInfo captures rooted Coordinator bootstrap results.
type BootstrapInfo struct {
	LoadedRegions int
	IDStart       uint64
	TSStart       uint64
	Snapshot      Snapshot
}
