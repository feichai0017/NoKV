package root

import "github.com/feichai0017/NoKV/raftstore/descriptor"

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
	LeftRegionID  uint64
	RightRegionID uint64
	Merged        descriptor.Descriptor
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
	RegionID      uint64
	FromPeerID    uint64
	ToPeerID      uint64
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

	StoreMembership  *StoreMembership
	RegionDescriptor *RegionDescriptorRecord
	RegionRemoval    *RegionRemoval
	RangeSplit       *RangeSplit
	RangeMerge       *RangeMerge
	PeerChange       *PeerChange
	LeaderTransfer   *LeaderTransfer
	PlacementPolicy  *PlacementPolicy
}

func StoreJoined(storeID uint64, address string) Event {
	return Event{
		Kind:            EventKindStoreJoined,
		StoreMembership: &StoreMembership{StoreID: storeID, Address: address},
	}
}

func StoreLeft(storeID uint64, address string) Event {
	return Event{
		Kind:            EventKindStoreLeft,
		StoreMembership: &StoreMembership{StoreID: storeID, Address: address},
	}
}

func StoreMarkedDraining(storeID uint64, address string) Event {
	return Event{
		Kind:            EventKindStoreMarkedDraining,
		StoreMembership: &StoreMembership{StoreID: storeID, Address: address},
	}
}

func RegionBootstrapped(desc descriptor.Descriptor) Event {
	return Event{
		Kind:             EventKindRegionBootstrap,
		RegionDescriptor: &RegionDescriptorRecord{Descriptor: desc},
	}
}

func RegionDescriptorPublished(desc descriptor.Descriptor) Event {
	return Event{
		Kind:             EventKindRegionDescriptorPublished,
		RegionDescriptor: &RegionDescriptorRecord{Descriptor: desc},
	}
}

func RegionTombstoned(regionID uint64) Event {
	return Event{
		Kind:          EventKindRegionTombstoned,
		RegionRemoval: &RegionRemoval{RegionID: regionID},
	}
}

func RegionSplitRequested(parentRegionID uint64, splitKey []byte, left, right descriptor.Descriptor) Event {
	return Event{
		Kind: EventKindRegionSplitRequested,
		RangeSplit: &RangeSplit{
			ParentRegionID: parentRegionID,
			SplitKey:       append([]byte(nil), splitKey...),
			Left:           left,
			Right:          right,
		},
	}
}

func RegionSplitCommitted(parentRegionID uint64, splitKey []byte, left, right descriptor.Descriptor) Event {
	return Event{
		Kind: EventKindRegionSplitCommitted,
		RangeSplit: &RangeSplit{
			ParentRegionID: parentRegionID,
			SplitKey:       append([]byte(nil), splitKey...),
			Left:           left,
			Right:          right,
		},
	}
}

func RegionMerged(leftRegionID, rightRegionID uint64, merged descriptor.Descriptor) Event {
	return Event{
		Kind: EventKindRegionMerged,
		RangeMerge: &RangeMerge{
			LeftRegionID:  leftRegionID,
			RightRegionID: rightRegionID,
			Merged:        merged,
		},
	}
}

func PeerAdded(regionID, storeID, peerID uint64, region descriptor.Descriptor) Event {
	return Event{
		Kind: EventKindPeerAdded,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}

func PeerRemoved(regionID, storeID, peerID uint64, region descriptor.Descriptor) Event {
	return Event{
		Kind: EventKindPeerRemoved,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}

func LeaderTransferPlanned(regionID, fromPeerID, toPeerID, targetStoreID uint64) Event {
	return Event{
		Kind: EventKindLeaderTransferIntent,
		LeaderTransfer: &LeaderTransfer{
			RegionID:      regionID,
			FromPeerID:    fromPeerID,
			ToPeerID:      toPeerID,
			TargetStoreID: targetStoreID,
		},
	}
}

func PlacementPolicyChanged(name string, version uint64) Event {
	return Event{
		Kind:            EventKindPlacementPolicyChanged,
		PlacementPolicy: &PlacementPolicy{Name: name, Version: version},
	}
}
