package codec

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

func RootCursorToProto(cursor rootpkg.Cursor) *metapb.RootCursor {
	return &metapb.RootCursor{Term: cursor.Term, Index: cursor.Index}
}

func RootCursorFromProto(pbCursor *metapb.RootCursor) rootpkg.Cursor {
	if pbCursor == nil {
		return rootpkg.Cursor{}
	}
	return rootpkg.Cursor{Term: pbCursor.Term, Index: pbCursor.Index}
}

func RootStateToProto(state rootpkg.State) *metapb.RootState {
	return &metapb.RootState{
		ClusterEpoch:    state.ClusterEpoch,
		MembershipEpoch: state.MembershipEpoch,
		PolicyVersion:   state.PolicyVersion,
		LastCommitted:   RootCursorToProto(state.LastCommitted),
		IdFence:         state.IDFence,
		TsoFence:        state.TSOFence,
	}
}

func RootStateFromProto(pbState *metapb.RootState) rootpkg.State {
	if pbState == nil {
		return rootpkg.State{}
	}
	return rootpkg.State{
		ClusterEpoch:    pbState.ClusterEpoch,
		MembershipEpoch: pbState.MembershipEpoch,
		PolicyVersion:   pbState.PolicyVersion,
		LastCommitted:   RootCursorFromProto(pbState.LastCommitted),
		IDFence:         pbState.IdFence,
		TSOFence:        pbState.TsoFence,
	}
}

func RootEventToProto(event rootpkg.Event) *metapb.RootEvent {
	pbEvent := &metapb.RootEvent{Kind: rootEventKindToProto(event.Kind)}
	switch {
	case event.StoreMembership != nil:
		pbEvent.Payload = &metapb.RootEvent_StoreMembership{StoreMembership: &metapb.RootStoreMembership{StoreId: event.StoreMembership.StoreID, Address: event.StoreMembership.Address}}
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
		}}
	case event.RangeMerge != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeMerge{RangeMerge: &metapb.RootRangeMerge{
			LeftRegionId:  event.RangeMerge.LeftRegionID,
			RightRegionId: event.RangeMerge.RightRegionID,
			Merged:        DescriptorToProto(event.RangeMerge.Merged),
		}}
	case event.PeerChange != nil:
		pbEvent.Payload = &metapb.RootEvent_PeerChange{PeerChange: &metapb.RootPeerChange{
			RegionId:    event.PeerChange.RegionID,
			StoreId:     event.PeerChange.StoreID,
			PeerId:      event.PeerChange.PeerID,
			Descriptor_: DescriptorToProto(event.PeerChange.Region),
		}}
	case event.LeaderTransfer != nil:
		pbEvent.Payload = &metapb.RootEvent_LeaderTransfer{LeaderTransfer: &metapb.RootLeaderTransfer{
			RegionId:      event.LeaderTransfer.RegionID,
			FromPeerId:    event.LeaderTransfer.FromPeerID,
			ToPeerId:      event.LeaderTransfer.ToPeerID,
			TargetStoreId: event.LeaderTransfer.TargetStoreID,
		}}
	case event.PlacementPolicy != nil:
		pbEvent.Payload = &metapb.RootEvent_PlacementPolicy{PlacementPolicy: &metapb.RootPlacementPolicy{
			Version: event.PlacementPolicy.Version,
			Name:    event.PlacementPolicy.Name,
		}}
	}
	return pbEvent
}

func RootEventFromProto(pbEvent *metapb.RootEvent) rootpkg.Event {
	if pbEvent == nil {
		return rootpkg.Event{}
	}
	event := rootpkg.Event{Kind: rootEventKindFromProto(pbEvent.Kind)}
	if body := pbEvent.GetStoreMembership(); body != nil {
		event.StoreMembership = &rootpkg.StoreMembership{StoreID: body.StoreId, Address: body.Address}
	}
	if body := pbEvent.GetRegionDescriptor(); body != nil {
		event.RegionDescriptor = &rootpkg.RegionDescriptorRecord{Descriptor: DescriptorFromProto(body.GetDescriptor_())}
	}
	if body := pbEvent.GetRegionRemoval(); body != nil {
		event.RegionRemoval = &rootpkg.RegionRemoval{RegionID: body.RegionId}
	}
	if body := pbEvent.GetRangeSplit(); body != nil {
		event.RangeSplit = &rootpkg.RangeSplit{
			ParentRegionID: body.ParentRegionId,
			SplitKey:       append([]byte(nil), body.SplitKey...),
			Left:           DescriptorFromProto(body.Left),
			Right:          DescriptorFromProto(body.Right),
		}
	}
	if body := pbEvent.GetRangeMerge(); body != nil {
		event.RangeMerge = &rootpkg.RangeMerge{
			LeftRegionID:  body.LeftRegionId,
			RightRegionID: body.RightRegionId,
			Merged:        DescriptorFromProto(body.Merged),
		}
	}
	if body := pbEvent.GetPeerChange(); body != nil {
		event.PeerChange = &rootpkg.PeerChange{
			RegionID: body.RegionId,
			StoreID:  body.StoreId,
			PeerID:   body.PeerId,
			Region:   DescriptorFromProto(body.GetDescriptor_()),
		}
	}
	if body := pbEvent.GetLeaderTransfer(); body != nil {
		event.LeaderTransfer = &rootpkg.LeaderTransfer{
			RegionID:      body.RegionId,
			FromPeerID:    body.FromPeerId,
			ToPeerID:      body.ToPeerId,
			TargetStoreID: body.TargetStoreId,
		}
	}
	if body := pbEvent.GetPlacementPolicy(); body != nil {
		event.PlacementPolicy = &rootpkg.PlacementPolicy{Version: body.Version, Name: body.Name}
	}
	return event
}

func RootCommitInfoToProto(commit rootpkg.CommitInfo) *metapb.RootCommitInfo {
	return &metapb.RootCommitInfo{
		Cursor: RootCursorToProto(commit.Cursor),
		State:  RootStateToProto(commit.State),
	}
}

func RootCommitInfoFromProto(pb *metapb.RootCommitInfo) rootpkg.CommitInfo {
	if pb == nil {
		return rootpkg.CommitInfo{}
	}
	return rootpkg.CommitInfo{
		Cursor: RootCursorFromProto(pb.Cursor),
		State:  RootStateFromProto(pb.State),
	}
}

func RootAllocatorKindToProto(kind rootpkg.AllocatorKind) metapb.AllocatorKind {
	switch kind {
	case rootpkg.AllocatorKindID:
		return metapb.AllocatorKind_ALLOCATOR_KIND_ID
	case rootpkg.AllocatorKindTSO:
		return metapb.AllocatorKind_ALLOCATOR_KIND_TSO
	default:
		return metapb.AllocatorKind_ALLOCATOR_KIND_UNSPECIFIED
	}
}

func RootAllocatorKindFromProto(kind metapb.AllocatorKind) rootpkg.AllocatorKind {
	switch kind {
	case metapb.AllocatorKind_ALLOCATOR_KIND_ID:
		return rootpkg.AllocatorKindID
	case metapb.AllocatorKind_ALLOCATOR_KIND_TSO:
		return rootpkg.AllocatorKindTSO
	default:
		return rootpkg.AllocatorKindUnknown
	}
}

func rootEventKindToProto(kind rootpkg.EventKind) metapb.RootEventKind {
	switch kind {
	case rootpkg.EventKindStoreJoined:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED
	case rootpkg.EventKindStoreLeft:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT
	case rootpkg.EventKindStoreMarkedDraining:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_MARKED_DRAINING
	case rootpkg.EventKindRegionBootstrap:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP
	case rootpkg.EventKindRegionDescriptorPublished:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED
	case rootpkg.EventKindRegionTombstoned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED
	case rootpkg.EventKindRegionSplitRequested:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_REQUESTED
	case rootpkg.EventKindRegionSplitCommitted:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED
	case rootpkg.EventKindRegionMerged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED
	case rootpkg.EventKindPeerAdded:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED
	case rootpkg.EventKindPeerRemoved:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED
	case rootpkg.EventKindLeaderTransferIntent:
		return metapb.RootEventKind_ROOT_EVENT_KIND_LEADER_TRANSFER_INTENT
	case rootpkg.EventKindPlacementPolicyChanged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PLACEMENT_POLICY_CHANGED
	default:
		return metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED
	}
}

func rootEventKindFromProto(kind metapb.RootEventKind) rootpkg.EventKind {
	switch kind {
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED:
		return rootpkg.EventKindStoreJoined
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT:
		return rootpkg.EventKindStoreLeft
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_MARKED_DRAINING:
		return rootpkg.EventKindStoreMarkedDraining
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP:
		return rootpkg.EventKindRegionBootstrap
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED:
		return rootpkg.EventKindRegionDescriptorPublished
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED:
		return rootpkg.EventKindRegionTombstoned
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_REQUESTED:
		return rootpkg.EventKindRegionSplitRequested
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED:
		return rootpkg.EventKindRegionSplitCommitted
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED:
		return rootpkg.EventKindRegionMerged
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED:
		return rootpkg.EventKindPeerAdded
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED:
		return rootpkg.EventKindPeerRemoved
	case metapb.RootEventKind_ROOT_EVENT_KIND_LEADER_TRANSFER_INTENT:
		return rootpkg.EventKindLeaderTransferIntent
	case metapb.RootEventKind_ROOT_EVENT_KIND_PLACEMENT_POLICY_CHANGED:
		return rootpkg.EventKindPlacementPolicyChanged
	default:
		return rootpkg.EventKindUnknown
	}
}
