package event

import (
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// Kind identifies one globally ordered metadata mutation.
type Kind uint16

const (
	KindUnknown Kind = iota
	KindStoreJoined
	KindStoreRetired
	KindIDAllocatorFenced
	KindTSOAllocatorFenced
	KindRegionBootstrap
	KindRegionDescriptorPublished
	KindRegionTombstoned
	KindRegionSplitPlanned
	KindRegionSplitCommitted
	KindRegionSplitCancelled
	KindRegionMergePlanned
	KindRegionMerged
	KindRegionMergeCancelled
	KindPeerAdditionPlanned
	KindPeerRemovalPlanned
	KindPeerAdded
	KindPeerRemoved
	KindPeerAdditionCancelled
	KindPeerRemovalCancelled
	KindTenure
	KindLegacy
	KindHandover
)

// StoreMembership describes one store membership change carried by a root event.
type StoreMembership struct {
	StoreID uint64
}

// AllocatorFence raises one rooted allocator floor monotonically.
type AllocatorFence struct {
	Minimum uint64
}

type RootCursor = rootproto.Cursor

// Tenure records the current control-plane owner lease.
type Tenure struct {
	HolderID        string
	ExpiresUnixNano int64
	Era             uint64
	IssuedAt        RootCursor
	Mandate         uint32
	LineageDigest   string
	Frontiers       rootproto.MandateFrontiers
}

// Legacy records one rooted handover point for the current control-plane
// authority era.
type Legacy struct {
	HolderID  string
	Era       uint64
	Mandate   uint32
	Frontiers rootproto.MandateFrontiers
	SealedAt  RootCursor
}

type HandoverStage = rootproto.HandoverStage

const (
	HandoverStageUnspecified = rootproto.HandoverStageUnspecified
	HandoverStageConfirmed   = rootproto.HandoverStageConfirmed
	HandoverStageClosed      = rootproto.HandoverStageClosed
	HandoverStageReattached  = rootproto.HandoverStageReattached
)

// Handover records one rooted handover lifecycle entry for a sealed
// predecessor era and its successor authority instance.
type Handover struct {
	HolderID     string
	LegacyEra    uint64
	SuccessorEra uint64
	LegacyDigest string
	Stage        HandoverStage
	ConfirmedAt  RootCursor
	ClosedAt     RootCursor
	ReattachedAt RootCursor
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
	BaseParent     descriptor.Descriptor
}

// RangeMerge describes one merge transition.
type RangeMerge struct {
	LeftRegionID  uint64
	RightRegionID uint64
	Merged        descriptor.Descriptor
	BaseLeft      descriptor.Descriptor
	BaseRight     descriptor.Descriptor
}

// PeerChange describes one region membership mutation.
type PeerChange struct {
	RegionID uint64
	StoreID  uint64
	PeerID   uint64
	Region   descriptor.Descriptor
	Base     descriptor.Descriptor
}

// Event is one globally ordered metadata-root mutation.
type Event struct {
	Kind Kind

	StoreMembership  *StoreMembership
	AllocatorFence   *AllocatorFence
	Tenure           *Tenure
	Legacy           *Legacy
	Handover         *Handover
	RegionDescriptor *RegionDescriptorRecord
	RegionRemoval    *RegionRemoval
	RangeSplit       *RangeSplit
	RangeMerge       *RangeMerge
	PeerChange       *PeerChange
}

func StoreJoined(storeID uint64) Event {
	return Event{Kind: KindStoreJoined, StoreMembership: &StoreMembership{StoreID: storeID}}
}

func StoreRetired(storeID uint64) Event {
	return Event{Kind: KindStoreRetired, StoreMembership: &StoreMembership{StoreID: storeID}}
}

func IDAllocatorFenced(min uint64) Event {
	return Event{Kind: KindIDAllocatorFenced, AllocatorFence: &AllocatorFence{Minimum: min}}
}

func TSOAllocatorFenced(min uint64) Event {
	return Event{Kind: KindTSOAllocatorFenced, AllocatorFence: &AllocatorFence{Minimum: min}}
}

func TenureGranted(holderID string, expiresUnixNano int64, era uint64, mandate uint32, lineageDigest string, frontiers rootproto.MandateFrontiers) Event {
	return newTenureEvent(holderID, expiresUnixNano, era, mandate, lineageDigest, frontiers)
}

func TenureReleased(holderID string, releasedUnixNano int64, era uint64, mandate uint32, lineageDigest string, frontiers rootproto.MandateFrontiers) Event {
	return newTenureEvent(holderID, releasedUnixNano, era, mandate, lineageDigest, frontiers)
}

func newTenureEvent(holderID string, expiresUnixNano int64, era uint64, mandate uint32, lineageDigest string, frontiers rootproto.MandateFrontiers) Event {
	return Event{
		Kind: KindTenure,
		Tenure: &Tenure{
			HolderID:        holderID,
			ExpiresUnixNano: expiresUnixNano,
			Era:             era,
			Mandate:         mandate,
			LineageDigest:   lineageDigest,
			Frontiers:       frontiers,
		},
	}
}

func TenureSealed(holderID string, era uint64, mandate uint32, frontiers rootproto.MandateFrontiers) Event {
	return Event{
		Kind: KindLegacy,
		Legacy: &Legacy{
			HolderID:  holderID,
			Era:       era,
			Mandate:   mandate,
			Frontiers: frontiers,
		},
	}
}

func HandoverConfirmed(holderID string, legacyEra, successorEra uint64, legacyDigest string) Event {
	return newHandoverEvent(holderID, legacyEra, successorEra, legacyDigest, HandoverStageConfirmed)
}

func HandoverClosed(holderID string, legacyEra, successorEra uint64, legacyDigest string) Event {
	return newHandoverEvent(holderID, legacyEra, successorEra, legacyDigest, HandoverStageClosed)
}

func HandoverReattached(holderID string, legacyEra, successorEra uint64, legacyDigest string) Event {
	return newHandoverEvent(holderID, legacyEra, successorEra, legacyDigest, HandoverStageReattached)
}

func newHandoverEvent(holderID string, legacyEra, successorEra uint64, legacyDigest string, stage HandoverStage) Event {
	return Event{
		Kind: KindHandover,
		Handover: &Handover{
			HolderID:     holderID,
			LegacyEra:    legacyEra,
			SuccessorEra: successorEra,
			LegacyDigest: legacyDigest,
			Stage:        stage,
		},
	}
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

func RegionSplitPlanned(parentRegionID uint64, splitKey []byte, left, right descriptor.Descriptor) Event {
	return Event{
		Kind: KindRegionSplitPlanned,
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
		Kind: KindRegionSplitCommitted,
		RangeSplit: &RangeSplit{
			ParentRegionID: parentRegionID,
			SplitKey:       append([]byte(nil), splitKey...),
			Left:           left,
			Right:          right,
		},
	}
}

func RegionSplitCancelled(parentRegionID uint64, splitKey []byte, left, right, base descriptor.Descriptor) Event {
	return Event{
		Kind: KindRegionSplitCancelled,
		RangeSplit: &RangeSplit{
			ParentRegionID: parentRegionID,
			SplitKey:       append([]byte(nil), splitKey...),
			Left:           left,
			Right:          right,
			BaseParent:     base,
		},
	}
}

func RegionMergePlanned(leftRegionID, rightRegionID uint64, merged descriptor.Descriptor) Event {
	return Event{
		Kind: KindRegionMergePlanned,
		RangeMerge: &RangeMerge{
			LeftRegionID:  leftRegionID,
			RightRegionID: rightRegionID,
			Merged:        merged,
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

func RegionMergeCancelled(leftRegionID, rightRegionID uint64, merged, baseLeft, baseRight descriptor.Descriptor) Event {
	return Event{
		Kind: KindRegionMergeCancelled,
		RangeMerge: &RangeMerge{
			LeftRegionID:  leftRegionID,
			RightRegionID: rightRegionID,
			Merged:        merged,
			BaseLeft:      baseLeft,
			BaseRight:     baseRight,
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

func PeerAdditionPlanned(regionID, storeID, peerID uint64, region descriptor.Descriptor) Event {
	return Event{
		Kind: KindPeerAdditionPlanned,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}

func PeerAdditionCancelled(regionID, storeID, peerID uint64, region, base descriptor.Descriptor) Event {
	return Event{
		Kind: KindPeerAdditionCancelled,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
			Base:     base,
		},
	}
}

func PeerRemovalPlanned(regionID, storeID, peerID uint64, region descriptor.Descriptor) Event {
	return Event{
		Kind: KindPeerRemovalPlanned,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}

func PeerRemovalCancelled(regionID, storeID, peerID uint64, region, base descriptor.Descriptor) Event {
	return Event{
		Kind: KindPeerRemovalCancelled,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
			Base:     base,
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
