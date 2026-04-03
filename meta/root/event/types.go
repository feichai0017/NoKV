package event

import "github.com/feichai0017/NoKV/raftstore/descriptor"

// Kind identifies one globally ordered metadata mutation.
type Kind uint16

const (
	KindUnknown Kind = iota
	KindStoreJoined
	KindStoreLeft
	KindIDAllocatorFenced
	KindTSOAllocatorFenced
	KindRegionBootstrap
	KindRegionDescriptorPublished
	KindRegionTombstoned
	KindRegionSplitCommitted
	KindRegionMerged
	KindPeerAdded
	KindPeerRemoved
)

// StoreMembership describes one store membership change carried by a root event.
type StoreMembership struct {
	StoreID uint64
	Address string
}

// AllocatorFence raises one rooted allocator floor monotonically.
type AllocatorFence struct {
	Minimum uint64
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

// Event is one globally ordered metadata-root mutation.
type Event struct {
	Kind Kind

	StoreMembership  *StoreMembership
	AllocatorFence   *AllocatorFence
	RegionDescriptor *RegionDescriptorRecord
	RegionRemoval    *RegionRemoval
	RangeSplit       *RangeSplit
	RangeMerge       *RangeMerge
	PeerChange       *PeerChange
}

func StoreJoined(storeID uint64, address string) Event {
	return Event{Kind: KindStoreJoined, StoreMembership: &StoreMembership{StoreID: storeID, Address: address}}
}

func StoreLeft(storeID uint64, address string) Event {
	return Event{Kind: KindStoreLeft, StoreMembership: &StoreMembership{StoreID: storeID, Address: address}}
}

func IDAllocatorFenced(min uint64) Event {
	return Event{Kind: KindIDAllocatorFenced, AllocatorFence: &AllocatorFence{Minimum: min}}
}

func TSOAllocatorFenced(min uint64) Event {
	return Event{Kind: KindTSOAllocatorFenced, AllocatorFence: &AllocatorFence{Minimum: min}}
}

func RegionBootstrapped(desc descriptor.Descriptor) Event {
	return Event{Kind: KindRegionBootstrap, RegionDescriptor: &RegionDescriptorRecord{Descriptor: desc}}
}

func RegionDescriptorPublished(desc descriptor.Descriptor) Event {
	return Event{Kind: KindRegionDescriptorPublished, RegionDescriptor: &RegionDescriptorRecord{Descriptor: desc}}
}

func RegionTombstoned(regionID uint64) Event {
	return Event{Kind: KindRegionTombstoned, RegionRemoval: &RegionRemoval{RegionID: regionID}}
}

func RegionSplitCommitted(parentRegionID uint64, splitKey []byte, left, right descriptor.Descriptor) Event {
	return Event{
		Kind: KindRegionSplitCommitted,
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
		Kind: KindRegionMerged,
		RangeMerge: &RangeMerge{
			LeftRegionID:  leftRegionID,
			RightRegionID: rightRegionID,
			Merged:        merged,
		},
	}
}

func PeerAdded(regionID, storeID, peerID uint64, region descriptor.Descriptor) Event {
	return Event{
		Kind: KindPeerAdded,
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
		Kind: KindPeerRemoved,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}
