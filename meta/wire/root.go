package wire

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

func RootCursorToProto(cursor rootstate.Cursor) *metapb.RootCursor {
	return &metapb.RootCursor{Term: cursor.Term, Index: cursor.Index}
}

func RootCursorFromProto(pbCursor *metapb.RootCursor) rootstate.Cursor {
	if pbCursor == nil {
		return rootstate.Cursor{}
	}
	return rootstate.Cursor{Term: pbCursor.Term, Index: pbCursor.Index}
}

func RootStateToProto(state rootstate.State) *metapb.RootState {
	return &metapb.RootState{
		ClusterEpoch:     state.ClusterEpoch,
		MembershipEpoch:  state.MembershipEpoch,
		LastCommitted:    RootCursorToProto(state.LastCommitted),
		IdFence:          state.IDFence,
		TsoFence:         state.TSOFence,
		CoordinatorLease: RootCoordinatorLeaseToProto(state.CoordinatorLease),
	}
}

func RootStateFromProto(pbState *metapb.RootState) rootstate.State {
	if pbState == nil {
		return rootstate.State{}
	}
	return rootstate.State{
		ClusterEpoch:     pbState.ClusterEpoch,
		MembershipEpoch:  pbState.MembershipEpoch,
		LastCommitted:    RootCursorFromProto(pbState.LastCommitted),
		IDFence:          pbState.IdFence,
		TSOFence:         pbState.TsoFence,
		CoordinatorLease: RootCoordinatorLeaseFromProto(pbState.GetCoordinatorLease()),
	}
}

func RootCoordinatorLeaseToProto(lease rootstate.CoordinatorLease) *metapb.RootCoordinatorLease {
	if lease.HolderID == "" && lease.ExpiresUnixNano == 0 && lease.IDFence == 0 && lease.TSOFence == 0 {
		return nil
	}
	return &metapb.RootCoordinatorLease{
		HolderId:        lease.HolderID,
		ExpiresUnixNano: lease.ExpiresUnixNano,
		IdFence:         lease.IDFence,
		TsoFence:        lease.TSOFence,
	}
}

func RootCoordinatorLeaseFromProto(lease *metapb.RootCoordinatorLease) rootstate.CoordinatorLease {
	if lease == nil {
		return rootstate.CoordinatorLease{}
	}
	return rootstate.CoordinatorLease{
		HolderID:        lease.GetHolderId(),
		ExpiresUnixNano: lease.GetExpiresUnixNano(),
		IDFence:         lease.GetIdFence(),
		TSOFence:        lease.GetTsoFence(),
	}
}

func RootSnapshotToProto(snapshot rootstate.Snapshot, tailOffset uint64) *metapb.RootCheckpoint {
	descriptors := make([]*metapb.RegionDescriptor, 0, len(snapshot.Descriptors))
	for _, desc := range snapshot.Descriptors {
		descriptors = append(descriptors, DescriptorToProto(desc))
	}
	pending := make([]*metapb.RootPendingPeerChange, 0, len(snapshot.PendingPeerChanges))
	for regionID, change := range snapshot.PendingPeerChanges {
		pending = append(pending, RootPendingPeerChangeToProto(regionID, change))
	}
	pendingRanges := make([]*metapb.RootPendingRangeChange, 0, len(snapshot.PendingRangeChanges))
	for regionID, change := range snapshot.PendingRangeChanges {
		pendingRanges = append(pendingRanges, RootPendingRangeChangeToProto(regionID, change))
	}
	return &metapb.RootCheckpoint{
		State:               RootStateToProto(snapshot.State),
		Descriptors:         descriptors,
		TailOffset:          tailOffset,
		PendingPeerChanges:  pending,
		PendingRangeChanges: pendingRanges,
	}
}

func RootSnapshotFromProto(pbCheckpoint *metapb.RootCheckpoint) (rootstate.Snapshot, uint64) {
	if pbCheckpoint == nil {
		return rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, 0
	}
	snapshot := rootstate.Snapshot{
		State:               RootStateFromProto(pbCheckpoint.State),
		Descriptors:         make(map[uint64]descriptor.Descriptor, len(pbCheckpoint.Descriptors)),
		PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange, len(pbCheckpoint.PendingPeerChanges)),
		PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange, len(pbCheckpoint.PendingRangeChanges)),
	}
	for _, pbDesc := range pbCheckpoint.Descriptors {
		desc := DescriptorFromProto(pbDesc)
		if desc.RegionID == 0 {
			continue
		}
		snapshot.Descriptors[desc.RegionID] = desc
	}
	for _, pbPending := range pbCheckpoint.PendingPeerChanges {
		regionID, change := RootPendingPeerChangeFromProto(pbPending)
		if regionID == 0 {
			continue
		}
		snapshot.PendingPeerChanges[regionID] = change
	}
	for _, pbPending := range pbCheckpoint.PendingRangeChanges {
		regionID, change := RootPendingRangeChangeFromProto(pbPending)
		if regionID == 0 {
			continue
		}
		snapshot.PendingRangeChanges[regionID] = change
	}
	return snapshot, pbCheckpoint.TailOffset
}

func RootPendingPeerChangeToProto(regionID uint64, change rootstate.PendingPeerChange) *metapb.RootPendingPeerChange {
	return &metapb.RootPendingPeerChange{
		RegionId: regionID,
		StoreId:  change.StoreID,
		PeerId:   change.PeerID,
		Kind:     rootPendingPeerChangeKindToProto(change.Kind),
		Target:   DescriptorToProto(change.Target),
		Base:     DescriptorToProto(change.Base),
	}
}

func RootPendingPeerChangeFromProto(pbPending *metapb.RootPendingPeerChange) (uint64, rootstate.PendingPeerChange) {
	if pbPending == nil || pbPending.GetRegionId() == 0 {
		return 0, rootstate.PendingPeerChange{}
	}
	return pbPending.GetRegionId(), rootstate.PendingPeerChange{
		Kind:    rootPendingPeerChangeKindFromProto(pbPending.GetKind()),
		StoreID: pbPending.GetStoreId(),
		PeerID:  pbPending.GetPeerId(),
		Base:    DescriptorFromProto(pbPending.GetBase()),
		Target:  DescriptorFromProto(pbPending.GetTarget()),
	}
}

func RootPendingRangeChangeToProto(regionID uint64, change rootstate.PendingRangeChange) *metapb.RootPendingRangeChange {
	return &metapb.RootPendingRangeChange{
		RegionId:       regionID,
		Kind:           rootPendingRangeChangeKindToProto(change.Kind),
		ParentRegionId: change.ParentRegionID,
		LeftRegionId:   change.LeftRegionID,
		RightRegionId:  change.RightRegionID,
		Left:           DescriptorToProto(change.Left),
		Right:          DescriptorToProto(change.Right),
		Merged:         DescriptorToProto(change.Merged),
		BaseParent:     DescriptorToProto(change.BaseParent),
		BaseLeft:       DescriptorToProto(change.BaseLeft),
		BaseRight:      DescriptorToProto(change.BaseRight),
	}
}

func RootPendingRangeChangeFromProto(pbPending *metapb.RootPendingRangeChange) (uint64, rootstate.PendingRangeChange) {
	if pbPending == nil || pbPending.GetRegionId() == 0 {
		return 0, rootstate.PendingRangeChange{}
	}
	return pbPending.GetRegionId(), rootstate.PendingRangeChange{
		Kind:           rootPendingRangeChangeKindFromProto(pbPending.GetKind()),
		ParentRegionID: pbPending.GetParentRegionId(),
		LeftRegionID:   pbPending.GetLeftRegionId(),
		RightRegionID:  pbPending.GetRightRegionId(),
		BaseParent:     DescriptorFromProto(pbPending.GetBaseParent()),
		BaseLeft:       DescriptorFromProto(pbPending.GetBaseLeft()),
		BaseRight:      DescriptorFromProto(pbPending.GetBaseRight()),
		Left:           DescriptorFromProto(pbPending.GetLeft()),
		Right:          DescriptorFromProto(pbPending.GetRight()),
		Merged:         DescriptorFromProto(pbPending.GetMerged()),
	}
}

func rootPendingPeerChangeKindToProto(kind rootstate.PendingPeerChangeKind) metapb.RootPendingPeerChangeKind {
	switch kind {
	case rootstate.PendingPeerChangeAddition:
		return metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_ADDITION
	case rootstate.PendingPeerChangeRemoval:
		return metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_REMOVAL
	default:
		return metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_UNSPECIFIED
	}
}

func rootPendingPeerChangeKindFromProto(kind metapb.RootPendingPeerChangeKind) rootstate.PendingPeerChangeKind {
	switch kind {
	case metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_ADDITION:
		return rootstate.PendingPeerChangeAddition
	case metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_REMOVAL:
		return rootstate.PendingPeerChangeRemoval
	default:
		return rootstate.PendingPeerChangeUnknown
	}
}

func rootPendingRangeChangeKindToProto(kind rootstate.PendingRangeChangeKind) metapb.RootPendingRangeChangeKind {
	switch kind {
	case rootstate.PendingRangeChangeSplit:
		return metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_SPLIT
	case rootstate.PendingRangeChangeMerge:
		return metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_MERGE
	default:
		return metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_UNSPECIFIED
	}
}

func rootPendingRangeChangeKindFromProto(kind metapb.RootPendingRangeChangeKind) rootstate.PendingRangeChangeKind {
	switch kind {
	case metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_SPLIT:
		return rootstate.PendingRangeChangeSplit
	case metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_MERGE:
		return rootstate.PendingRangeChangeMerge
	default:
		return rootstate.PendingRangeChangeUnknown
	}
}

func RootEventToProto(event rootevent.Event) *metapb.RootEvent {
	pbEvent := &metapb.RootEvent{Kind: rootEventKindToProto(event.Kind)}
	switch {
	case event.StoreMembership != nil:
		pbEvent.Payload = &metapb.RootEvent_StoreMembership{StoreMembership: &metapb.RootStoreMembership{StoreId: event.StoreMembership.StoreID, Address: event.StoreMembership.Address}}
	case event.AllocatorFence != nil:
		pbEvent.Payload = &metapb.RootEvent_AllocatorFence{AllocatorFence: &metapb.RootAllocatorFence{Minimum: event.AllocatorFence.Minimum}}
	case event.CoordinatorLease != nil:
		pbEvent.Payload = &metapb.RootEvent_CoordinatorLease{CoordinatorLease: &metapb.RootCoordinatorLease{
			HolderId:        event.CoordinatorLease.HolderID,
			ExpiresUnixNano: event.CoordinatorLease.ExpiresUnixNano,
			IdFence:         event.CoordinatorLease.IDFence,
			TsoFence:        event.CoordinatorLease.TSOFence,
		}}
	case event.RegionDescriptor != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionDescriptor{RegionDescriptor: &metapb.RootRegionDescriptor{Descriptor_: DescriptorToProto(event.RegionDescriptor.Descriptor)}}
	case event.RegionRemoval != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionRemoval{RegionRemoval: &metapb.RootRegionRemoval{RegionId: event.RegionRemoval.RegionID}}
	case event.RangeSplit != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeSplit{RangeSplit: &metapb.RootRangeSplit{
			ParentRegionId: event.RangeSplit.ParentRegionID,
			SplitKey:       append([]byte(nil), event.RangeSplit.SplitKey...),
			Left:           DescriptorToProto(event.RangeSplit.Left),
			Right:          DescriptorToProto(event.RangeSplit.Right),
			BaseParent:     DescriptorToProto(event.RangeSplit.BaseParent),
		}}
	case event.RangeMerge != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeMerge{RangeMerge: &metapb.RootRangeMerge{
			LeftRegionId:  event.RangeMerge.LeftRegionID,
			RightRegionId: event.RangeMerge.RightRegionID,
			Merged:        DescriptorToProto(event.RangeMerge.Merged),
			BaseLeft:      DescriptorToProto(event.RangeMerge.BaseLeft),
			BaseRight:     DescriptorToProto(event.RangeMerge.BaseRight),
		}}
	case event.PeerChange != nil:
		pbEvent.Payload = &metapb.RootEvent_PeerChange{PeerChange: &metapb.RootPeerChange{
			RegionId: event.PeerChange.RegionID,
			StoreId:  event.PeerChange.StoreID,
			PeerId:   event.PeerChange.PeerID,
			Target:   DescriptorToProto(event.PeerChange.Region),
			Base:     DescriptorToProto(event.PeerChange.Base),
		}}
	}
	return pbEvent
}

func RootEventFromProto(pbEvent *metapb.RootEvent) rootevent.Event {
	if pbEvent == nil {
		return rootevent.Event{}
	}
	event := rootevent.Event{Kind: rootEventKindFromProto(pbEvent.Kind)}
	if body := pbEvent.GetStoreMembership(); body != nil {
		event.StoreMembership = &rootevent.StoreMembership{StoreID: body.StoreId, Address: body.Address}
	}
	if body := pbEvent.GetAllocatorFence(); body != nil {
		event.AllocatorFence = &rootevent.AllocatorFence{Minimum: body.Minimum}
	}
	if body := pbEvent.GetCoordinatorLease(); body != nil {
		event.CoordinatorLease = &rootevent.CoordinatorLease{
			HolderID:        body.GetHolderId(),
			ExpiresUnixNano: body.GetExpiresUnixNano(),
			IDFence:         body.GetIdFence(),
			TSOFence:        body.GetTsoFence(),
		}
	}
	if body := pbEvent.GetRegionDescriptor(); body != nil {
		event.RegionDescriptor = &rootevent.RegionDescriptorRecord{Descriptor: DescriptorFromProto(body.GetDescriptor_())}
	}
	if body := pbEvent.GetRegionRemoval(); body != nil {
		event.RegionRemoval = &rootevent.RegionRemoval{RegionID: body.RegionId}
	}
	if body := pbEvent.GetRangeSplit(); body != nil {
		event.RangeSplit = &rootevent.RangeSplit{
			ParentRegionID: body.ParentRegionId,
			SplitKey:       append([]byte(nil), body.SplitKey...),
			Left:           DescriptorFromProto(body.Left),
			Right:          DescriptorFromProto(body.Right),
			BaseParent:     DescriptorFromProto(body.BaseParent),
		}
	}
	if body := pbEvent.GetRangeMerge(); body != nil {
		event.RangeMerge = &rootevent.RangeMerge{
			LeftRegionID:  body.LeftRegionId,
			RightRegionID: body.RightRegionId,
			Merged:        DescriptorFromProto(body.Merged),
			BaseLeft:      DescriptorFromProto(body.BaseLeft),
			BaseRight:     DescriptorFromProto(body.BaseRight),
		}
	}
	if body := pbEvent.GetPeerChange(); body != nil {
		event.PeerChange = &rootevent.PeerChange{
			RegionID: body.RegionId,
			StoreID:  body.StoreId,
			PeerID:   body.PeerId,
			Region:   DescriptorFromProto(body.GetTarget()),
			Base:     DescriptorFromProto(body.GetBase()),
		}
	}
	return event
}

func rootEventKindToProto(kind rootevent.Kind) metapb.RootEventKind {
	switch kind {
	case rootevent.KindStoreJoined:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED
	case rootevent.KindStoreLeft:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT
	case rootevent.KindIDAllocatorFenced:
		return metapb.RootEventKind_ROOT_EVENT_KIND_ID_ALLOCATOR_FENCED
	case rootevent.KindRegionBootstrap:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP
	case rootevent.KindRegionDescriptorPublished:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED
	case rootevent.KindRegionTombstoned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED
	case rootevent.KindTSOAllocatorFenced:
		return metapb.RootEventKind_ROOT_EVENT_KIND_TSO_ALLOCATOR_FENCED
	case rootevent.KindRegionSplitPlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_PLANNED
	case rootevent.KindRegionSplitCommitted:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED
	case rootevent.KindRegionSplitCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_CANCELLED
	case rootevent.KindRegionMergePlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_PLANNED
	case rootevent.KindRegionMerged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED
	case rootevent.KindRegionMergeCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_CANCELLED
	case rootevent.KindPeerAdditionPlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_PLANNED
	case rootevent.KindPeerRemovalPlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_PLANNED
	case rootevent.KindPeerAdded:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED
	case rootevent.KindPeerRemoved:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED
	case rootevent.KindPeerAdditionCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_CANCELLED
	case rootevent.KindPeerRemovalCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_CANCELLED
	case rootevent.KindCoordinatorLease:
		return metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_LEASE
	default:
		return metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED
	}
}

func rootEventKindFromProto(kind metapb.RootEventKind) rootevent.Kind {
	switch kind {
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED:
		return rootevent.KindStoreJoined
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT:
		return rootevent.KindStoreLeft
	case metapb.RootEventKind_ROOT_EVENT_KIND_ID_ALLOCATOR_FENCED:
		return rootevent.KindIDAllocatorFenced
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP:
		return rootevent.KindRegionBootstrap
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED:
		return rootevent.KindRegionDescriptorPublished
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED:
		return rootevent.KindRegionTombstoned
	case metapb.RootEventKind_ROOT_EVENT_KIND_TSO_ALLOCATOR_FENCED:
		return rootevent.KindTSOAllocatorFenced
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_PLANNED:
		return rootevent.KindRegionSplitPlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED:
		return rootevent.KindRegionSplitCommitted
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_CANCELLED:
		return rootevent.KindRegionSplitCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_PLANNED:
		return rootevent.KindRegionMergePlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED:
		return rootevent.KindRegionMerged
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_CANCELLED:
		return rootevent.KindRegionMergeCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_PLANNED:
		return rootevent.KindPeerAdditionPlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_PLANNED:
		return rootevent.KindPeerRemovalPlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED:
		return rootevent.KindPeerAdded
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED:
		return rootevent.KindPeerRemoved
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_CANCELLED:
		return rootevent.KindPeerAdditionCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_CANCELLED:
		return rootevent.KindPeerRemovalCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_LEASE:
		return rootevent.KindCoordinatorLease
	default:
		return rootevent.KindUnknown
	}
}
