package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// Cursor identifies one committed position in the metadata-root log.
type Cursor struct {
	Term  uint64
	Index uint64
}

// State is the compact checkpointed state of the metadata root.
type State struct {
	ClusterEpoch    uint64
	MembershipEpoch uint64
	LastCommitted   Cursor
	IDFence         uint64
	TSOFence        uint64
}

// Snapshot is the compact materialized rooted metadata state used for bounded bootstrap and recovery.
type Snapshot struct {
	State       State
	Descriptors map[uint64]descriptor.Descriptor
}

// CommitInfo reports one successful root append together with the resulting compact root state.
type CommitInfo struct {
	Cursor Cursor
	State  State
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	out := Snapshot{State: snapshot.State, Descriptors: CloneDescriptors(snapshot.Descriptors)}
	return out
}

func CloneDescriptors(in map[uint64]descriptor.Descriptor) map[uint64]descriptor.Descriptor {
	if len(in) == 0 {
		return make(map[uint64]descriptor.Descriptor)
	}
	out := make(map[uint64]descriptor.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

// ApplyEventToState applies one rooted metadata event into compact root state.
func ApplyEventToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil {
		return
	}
	switch event.Kind {
	case rootevent.KindStoreJoined, rootevent.KindStoreLeft:
		state.MembershipEpoch++
	case rootevent.KindIDAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.IDFence {
			state.IDFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTSOAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.TSOFence {
			state.TSOFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindRegionBootstrap,
		rootevent.KindRegionDescriptorPublished,
		rootevent.KindRegionTombstoned,
		rootevent.KindRegionSplitCommitted,
		rootevent.KindRegionMerged,
		rootevent.KindPeerAdded,
		rootevent.KindPeerRemoved:
		state.ClusterEpoch++
	}
	state.LastCommitted = cursor
}

// NextCursor returns the next ordered root cursor.
func NextCursor(prev Cursor) Cursor {
	term := prev.Term
	if term == 0 {
		term = 1
	}
	return Cursor{Term: term, Index: prev.Index + 1}
}

// CursorAfter reports whether a is ordered strictly after b.
func CursorAfter(a, b Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
}
