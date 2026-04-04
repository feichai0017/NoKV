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

type PendingPeerChangeKind uint8

const (
	PendingPeerChangeUnknown PendingPeerChangeKind = iota
	PendingPeerChangeAddition
	PendingPeerChangeRemoval
)

type PendingPeerChange struct {
	Kind    PendingPeerChangeKind
	StoreID uint64
	PeerID  uint64
	Target  descriptor.Descriptor
}

type PendingRangeChangeKind uint8

const (
	PendingRangeChangeUnknown PendingRangeChangeKind = iota
	PendingRangeChangeSplit
	PendingRangeChangeMerge
)

type PendingRangeChange struct {
	Kind           PendingRangeChangeKind
	ParentRegionID uint64
	LeftRegionID   uint64
	RightRegionID  uint64
	Left           descriptor.Descriptor
	Right          descriptor.Descriptor
	Merged         descriptor.Descriptor
}

// Snapshot is the compact materialized rooted metadata state used for bounded bootstrap and recovery.
type Snapshot struct {
	State               State
	Descriptors         map[uint64]descriptor.Descriptor
	PendingPeerChanges  map[uint64]PendingPeerChange
	PendingRangeChanges map[uint64]PendingRangeChange
}

// CommitInfo reports one successful root append together with the resulting compact root state.
type CommitInfo struct {
	Cursor Cursor
	State  State
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	out := Snapshot{
		State:               snapshot.State,
		Descriptors:         CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: ClonePendingRangeChanges(snapshot.PendingRangeChanges),
	}
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

func ClonePendingPeerChanges(in map[uint64]PendingPeerChange) map[uint64]PendingPeerChange {
	if len(in) == 0 {
		return make(map[uint64]PendingPeerChange)
	}
	out := make(map[uint64]PendingPeerChange, len(in))
	for id, change := range in {
		out[id] = PendingPeerChange{
			Kind:    change.Kind,
			StoreID: change.StoreID,
			PeerID:  change.PeerID,
			Target:  change.Target.Clone(),
		}
	}
	return out
}

func ClonePendingRangeChanges(in map[uint64]PendingRangeChange) map[uint64]PendingRangeChange {
	if len(in) == 0 {
		return make(map[uint64]PendingRangeChange)
	}
	out := make(map[uint64]PendingRangeChange, len(in))
	for id, change := range in {
		out[id] = PendingRangeChange{
			Kind:           change.Kind,
			ParentRegionID: change.ParentRegionID,
			LeftRegionID:   change.LeftRegionID,
			RightRegionID:  change.RightRegionID,
			Left:           change.Left.Clone(),
			Right:          change.Right.Clone(),
			Merged:         change.Merged.Clone(),
		}
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
		rootevent.KindRegionSplitPlanned,
		rootevent.KindRegionSplitCommitted,
		rootevent.KindRegionMergePlanned,
		rootevent.KindRegionMerged,
		rootevent.KindPeerAdditionPlanned,
		rootevent.KindPeerRemovalPlanned,
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
