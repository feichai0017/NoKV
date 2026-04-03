package root

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

type EventKind = rootevent.Kind

type StoreMembership = rootevent.StoreMembership
type RegionDescriptorRecord = rootevent.RegionDescriptorRecord
type RegionRemoval = rootevent.RegionRemoval
type RangeSplit = rootevent.RangeSplit
type RangeMerge = rootevent.RangeMerge
type PeerChange = rootevent.PeerChange
type LeaderTransfer = rootevent.LeaderTransfer
type PlacementPolicy = rootevent.PlacementPolicy
type Event = rootevent.Event

const (
	EventKindUnknown                   EventKind = rootevent.KindUnknown
	EventKindStoreJoined               EventKind = rootevent.KindStoreJoined
	EventKindStoreLeft                 EventKind = rootevent.KindStoreLeft
	EventKindStoreMarkedDraining       EventKind = rootevent.KindStoreMarkedDraining
	EventKindRegionBootstrap           EventKind = rootevent.KindRegionBootstrap
	EventKindRegionDescriptorPublished EventKind = rootevent.KindRegionDescriptorPublished
	EventKindRegionTombstoned          EventKind = rootevent.KindRegionTombstoned
	EventKindRegionSplitRequested      EventKind = rootevent.KindRegionSplitRequested
	EventKindRegionSplitCommitted      EventKind = rootevent.KindRegionSplitCommitted
	EventKindRegionMerged              EventKind = rootevent.KindRegionMerged
	EventKindPeerAdded                 EventKind = rootevent.KindPeerAdded
	EventKindPeerRemoved               EventKind = rootevent.KindPeerRemoved
	EventKindLeaderTransferIntent      EventKind = rootevent.KindLeaderTransferIntent
	EventKindPlacementPolicyChanged    EventKind = rootevent.KindPlacementPolicyChanged
)

var (
	StoreJoined               = rootevent.StoreJoined
	StoreLeft                 = rootevent.StoreLeft
	StoreMarkedDraining       = rootevent.StoreMarkedDraining
	RegionBootstrapped        = rootevent.RegionBootstrapped
	RegionDescriptorPublished = rootevent.RegionDescriptorPublished
	RegionTombstoned          = rootevent.RegionTombstoned
	RegionSplitRequested      = rootevent.RegionSplitRequested
	RegionSplitCommitted      = rootevent.RegionSplitCommitted
	RegionMerged              = rootevent.RegionMerged
	PeerAdded                 = rootevent.PeerAdded
	PeerRemoved               = rootevent.PeerRemoved
	LeaderTransferPlanned     = rootevent.LeaderTransferPlanned
	PlacementPolicyChanged    = rootevent.PlacementPolicyChanged
)
