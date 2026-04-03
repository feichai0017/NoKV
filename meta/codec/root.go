package codec

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
		ClusterEpoch:    state.ClusterEpoch,
		MembershipEpoch: state.MembershipEpoch,
		PolicyVersion:   state.PolicyVersion,
		LastCommitted:   RootCursorToProto(state.LastCommitted),
		IdFence:         state.IDFence,
		TsoFence:        state.TSOFence,
	}
}

func RootStateFromProto(pbState *metapb.RootState) rootstate.State {
	if pbState == nil {
		return rootstate.State{}
	}
	return rootstate.State{
		ClusterEpoch:    pbState.ClusterEpoch,
		MembershipEpoch: pbState.MembershipEpoch,
		PolicyVersion:   pbState.PolicyVersion,
		LastCommitted:   RootCursorFromProto(pbState.LastCommitted),
		IDFence:         pbState.IdFence,
		TSOFence:        pbState.TsoFence,
	}
}

func RootSnapshotToProto(snapshot rootstate.Snapshot, logOffset uint64) *metapb.RootCheckpoint {
	descriptors := make([]*metapb.RegionDescriptor, 0, len(snapshot.Descriptors))
	for _, desc := range snapshot.Descriptors {
		descriptors = append(descriptors, DescriptorToProto(desc))
	}
	return &metapb.RootCheckpoint{
		State:       RootStateToProto(snapshot.State),
		Descriptors: descriptors,
		LogOffset:   logOffset,
	}
}

func RootSnapshotFromProto(pbCheckpoint *metapb.RootCheckpoint) (rootstate.Snapshot, uint64) {
	if pbCheckpoint == nil {
		return rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, 0
	}
	snapshot := rootstate.Snapshot{
		State:       RootStateFromProto(pbCheckpoint.State),
		Descriptors: make(map[uint64]descriptor.Descriptor, len(pbCheckpoint.Descriptors)),
	}
	for _, pbDesc := range pbCheckpoint.Descriptors {
		desc := DescriptorFromProto(pbDesc)
		if desc.RegionID == 0 {
			continue
		}
		snapshot.Descriptors[desc.RegionID] = desc
	}
	return snapshot, pbCheckpoint.LogOffset
}

func RootEventToProto(event rootevent.Event) *metapb.RootEvent {
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

func RootEventFromProto(pbEvent *metapb.RootEvent) rootevent.Event {
	if pbEvent == nil {
		return rootevent.Event{}
	}
	event := rootevent.Event{Kind: rootEventKindFromProto(pbEvent.Kind)}
	if body := pbEvent.GetStoreMembership(); body != nil {
		event.StoreMembership = &rootevent.StoreMembership{StoreID: body.StoreId, Address: body.Address}
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
		}
	}
	if body := pbEvent.GetRangeMerge(); body != nil {
		event.RangeMerge = &rootevent.RangeMerge{
			LeftRegionID:  body.LeftRegionId,
			RightRegionID: body.RightRegionId,
			Merged:        DescriptorFromProto(body.Merged),
		}
	}
	if body := pbEvent.GetPeerChange(); body != nil {
		event.PeerChange = &rootevent.PeerChange{
			RegionID: body.RegionId,
			StoreID:  body.StoreId,
			PeerID:   body.PeerId,
			Region:   DescriptorFromProto(body.GetDescriptor_()),
		}
	}
	if body := pbEvent.GetLeaderTransfer(); body != nil {
		event.LeaderTransfer = &rootevent.LeaderTransfer{
			RegionID:      body.RegionId,
			FromPeerID:    body.FromPeerId,
			ToPeerID:      body.ToPeerId,
			TargetStoreID: body.TargetStoreId,
		}
	}
	if body := pbEvent.GetPlacementPolicy(); body != nil {
		event.PlacementPolicy = &rootevent.PlacementPolicy{Version: body.Version, Name: body.Name}
	}
	return event
}

func rootEventKindToProto(kind rootevent.Kind) metapb.RootEventKind {
	switch kind {
	case rootevent.KindStoreJoined:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED
	case rootevent.KindStoreLeft:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT
	case rootevent.KindStoreMarkedDraining:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_MARKED_DRAINING
	case rootevent.KindRegionBootstrap:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP
	case rootevent.KindRegionDescriptorPublished:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED
	case rootevent.KindRegionTombstoned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED
	case rootevent.KindRegionSplitRequested:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_REQUESTED
	case rootevent.KindRegionSplitCommitted:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED
	case rootevent.KindRegionMerged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED
	case rootevent.KindPeerAdded:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED
	case rootevent.KindPeerRemoved:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED
	case rootevent.KindLeaderTransferIntent:
		return metapb.RootEventKind_ROOT_EVENT_KIND_LEADER_TRANSFER_INTENT
	case rootevent.KindPlacementPolicyChanged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PLACEMENT_POLICY_CHANGED
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
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_MARKED_DRAINING:
		return rootevent.KindStoreMarkedDraining
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP:
		return rootevent.KindRegionBootstrap
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED:
		return rootevent.KindRegionDescriptorPublished
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED:
		return rootevent.KindRegionTombstoned
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_REQUESTED:
		return rootevent.KindRegionSplitRequested
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED:
		return rootevent.KindRegionSplitCommitted
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED:
		return rootevent.KindRegionMerged
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED:
		return rootevent.KindPeerAdded
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED:
		return rootevent.KindPeerRemoved
	case metapb.RootEventKind_ROOT_EVENT_KIND_LEADER_TRANSFER_INTENT:
		return rootevent.KindLeaderTransferIntent
	case metapb.RootEventKind_ROOT_EVENT_KIND_PLACEMENT_POLICY_CHANGED:
		return rootevent.KindPlacementPolicyChanged
	default:
		return rootevent.KindUnknown
	}
}
