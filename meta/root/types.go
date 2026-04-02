package root

import "github.com/feichai0017/NoKV/raftstore/descriptor"

// AllocatorKind identifies one globally fenced allocator domain.
type AllocatorKind uint8

const (
	AllocatorKindUnknown AllocatorKind = iota
	AllocatorKindID
	AllocatorKindTSO
)

// Cursor identifies one committed position in the metadata-root log.
type Cursor struct {
	Term  uint64
	Index uint64
}

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

// CommitInfo reports one successful root append together with the resulting
// compact root state.
type CommitInfo struct {
	Cursor Cursor
	State  State
}

// EventKind identifies one globally ordered metadata mutation.
type EventKind uint16

const (
	EventKindUnknown EventKind = iota
	EventKindStoreJoined
	EventKindStoreLeft
	EventKindStoreMarkedDraining
	EventKindRegionBootstrap
	EventKindRegionDescriptorPublished
	EventKindRegionTombstoned
	EventKindRegionSplitRequested
	EventKindRegionSplitCommitted
	EventKindRegionMerged
	EventKindPeerAdded
	EventKindPeerRemoved
	EventKindLeaderTransferIntent
	EventKindPlacementPolicyChanged
)

// StoreMembership describes one store membership change carried by a root
// event.
type StoreMembership struct {
	StoreID uint64
	Address string
}

// RegionDescriptorRecord carries one descriptor snapshot into the root log.
type RegionDescriptorRecord struct {
	Descriptor descriptor.Descriptor
}

// RegionRemoval removes one region descriptor from the rooted topology view.
type RegionRemoval struct {
	RegionID uint64
}

// RangeSplit describes one split intent or committed split transition.
type RangeSplit struct {
	ParentRegionID uint64
	SplitKey       []byte
	Left           descriptor.Descriptor
	Right          descriptor.Descriptor
}

// RangeMerge describes one merge transition.
type RangeMerge struct {
	LeftRegionID   uint64
	RightRegionID  uint64
	Merged         descriptor.Descriptor
}

// PeerChange describes one region membership mutation.
type PeerChange struct {
	RegionID uint64
	StoreID  uint64
	PeerID   uint64
	Region   descriptor.Descriptor
}

// LeaderTransfer describes one leadership movement intent.
type LeaderTransfer struct {
	RegionID     uint64
	FromPeerID   uint64
	ToPeerID     uint64
	TargetStoreID uint64
}

// PlacementPolicy describes a cluster policy-version change.
type PlacementPolicy struct {
	Version uint64
	Name    string
}

// Event is one globally ordered metadata-root mutation.
//
// The body is intentionally sparse. Future runtime wiring can decide whether to
// keep this union-like shape or hide it behind typed constructors.
type Event struct {
	Kind EventKind

	StoreMembership *StoreMembership
	RegionDescriptor *RegionDescriptorRecord
	RegionRemoval   *RegionRemoval
	RangeSplit      *RangeSplit
	RangeMerge      *RangeMerge
	PeerChange      *PeerChange
	LeaderTransfer  *LeaderTransfer
	PlacementPolicy *PlacementPolicy
}

// Root is the minimal interface exposed by a metadata-root implementation.
//
// The implementation may be local, replicated, or mock-backed. Callers should
// only depend on ordered events, compact checkpoints, and allocator fencing.
type Root interface {
	Current() (State, error)
	ReadSince(cursor Cursor) ([]Event, Cursor, error)
	Append(events ...Event) (CommitInfo, error)
	FenceAllocator(kind AllocatorKind, min uint64) (uint64, error)
}
