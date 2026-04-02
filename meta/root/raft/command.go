package rootraft

import (
	"encoding/binary"
	"fmt"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/protobuf/proto"
)

type commandKind uint8

const (
	commandKindUnknown commandKind = iota
	commandKindEvent
	commandKindFence
)

type command struct {
	kind  commandKind
	event rootpkg.Event
	fence allocatorFence
}

type allocatorFence struct {
	kind rootpkg.AllocatorKind
	min  uint64
}

func encodeEventCommand(event rootpkg.Event) ([]byte, error) {
	payload, err := proto.Marshal(eventToPB(event))
	if err != nil {
		return nil, err
	}
	out := make([]byte, 1+len(payload))
	out[0] = byte(commandKindEvent)
	copy(out[1:], payload)
	return out, nil
}

func encodeFenceCommand(kind rootpkg.AllocatorKind, min uint64) []byte {
	out := make([]byte, 10)
	out[0] = byte(commandKindFence)
	out[1] = byte(kind)
	binary.LittleEndian.PutUint64(out[2:10], min)
	return out
}

func decodeCommand(data []byte) (command, error) {
	if len(data) == 0 {
		return command{}, nil
	}
	switch commandKind(data[0]) {
	case commandKindEvent:
		var pbEvent metapb.RootEvent
		if err := proto.Unmarshal(data[1:], &pbEvent); err != nil {
			return command{}, err
		}
		return command{kind: commandKindEvent, event: eventFromPB(&pbEvent)}, nil
	case commandKindFence:
		if len(data) != 10 {
			return command{}, fmt.Errorf("meta/root/raft: invalid fence command payload")
		}
		return command{kind: commandKindFence, fence: allocatorFence{kind: rootpkg.AllocatorKind(data[1]), min: binary.LittleEndian.Uint64(data[2:10])}}, nil
	default:
		return command{}, fmt.Errorf("meta/root/raft: unknown command kind %d", data[0])
	}
}

func eventToPB(event rootpkg.Event) *metapb.RootEvent {
	pbEvent := &metapb.RootEvent{Kind: eventKindToPB(event.Kind)}
	switch {
	case event.StoreMembership != nil:
		pbEvent.Payload = &metapb.RootEvent_StoreMembership{StoreMembership: &metapb.RootStoreMembership{StoreId: event.StoreMembership.StoreID, Address: event.StoreMembership.Address}}
	case event.RegionDescriptor != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionDescriptor{RegionDescriptor: &metapb.RootRegionDescriptor{Descriptor_: metacodec.DescriptorToProto(event.RegionDescriptor.Descriptor)}}
	case event.RegionRemoval != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionRemoval{RegionRemoval: &metapb.RootRegionRemoval{RegionId: event.RegionRemoval.RegionID}}
	case event.RangeSplit != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeSplit{RangeSplit: &metapb.RootRangeSplit{ParentRegionId: event.RangeSplit.ParentRegionID, SplitKey: append([]byte(nil), event.RangeSplit.SplitKey...), Left: metacodec.DescriptorToProto(event.RangeSplit.Left), Right: metacodec.DescriptorToProto(event.RangeSplit.Right)}}
	case event.RangeMerge != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeMerge{RangeMerge: &metapb.RootRangeMerge{LeftRegionId: event.RangeMerge.LeftRegionID, RightRegionId: event.RangeMerge.RightRegionID, Merged: metacodec.DescriptorToProto(event.RangeMerge.Merged)}}
	case event.PeerChange != nil:
		pbEvent.Payload = &metapb.RootEvent_PeerChange{PeerChange: &metapb.RootPeerChange{RegionId: event.PeerChange.RegionID, StoreId: event.PeerChange.StoreID, PeerId: event.PeerChange.PeerID, Descriptor_: metacodec.DescriptorToProto(event.PeerChange.Region)}}
	case event.LeaderTransfer != nil:
		pbEvent.Payload = &metapb.RootEvent_LeaderTransfer{LeaderTransfer: &metapb.RootLeaderTransfer{RegionId: event.LeaderTransfer.RegionID, FromPeerId: event.LeaderTransfer.FromPeerID, ToPeerId: event.LeaderTransfer.ToPeerID, TargetStoreId: event.LeaderTransfer.TargetStoreID}}
	case event.PlacementPolicy != nil:
		pbEvent.Payload = &metapb.RootEvent_PlacementPolicy{PlacementPolicy: &metapb.RootPlacementPolicy{Version: event.PlacementPolicy.Version, Name: event.PlacementPolicy.Name}}
	}
	return pbEvent
}

func eventFromPB(pbEvent *metapb.RootEvent) rootpkg.Event {
	if pbEvent == nil {
		return rootpkg.Event{}
	}
	event := rootpkg.Event{Kind: eventKindFromPB(pbEvent.Kind)}
	if body := pbEvent.GetStoreMembership(); body != nil {
		event.StoreMembership = &rootpkg.StoreMembership{StoreID: body.StoreId, Address: body.Address}
	}
	if body := pbEvent.GetRegionDescriptor(); body != nil {
		event.RegionDescriptor = &rootpkg.RegionDescriptorRecord{Descriptor: metacodec.DescriptorFromProto(body.GetDescriptor_())}
	}
	if body := pbEvent.GetRegionRemoval(); body != nil {
		event.RegionRemoval = &rootpkg.RegionRemoval{RegionID: body.RegionId}
	}
	if body := pbEvent.GetRangeSplit(); body != nil {
		event.RangeSplit = &rootpkg.RangeSplit{ParentRegionID: body.ParentRegionId, SplitKey: append([]byte(nil), body.SplitKey...), Left: metacodec.DescriptorFromProto(body.Left), Right: metacodec.DescriptorFromProto(body.Right)}
	}
	if body := pbEvent.GetRangeMerge(); body != nil {
		event.RangeMerge = &rootpkg.RangeMerge{LeftRegionID: body.LeftRegionId, RightRegionID: body.RightRegionId, Merged: metacodec.DescriptorFromProto(body.Merged)}
	}
	if body := pbEvent.GetPeerChange(); body != nil {
		event.PeerChange = &rootpkg.PeerChange{RegionID: body.RegionId, StoreID: body.StoreId, PeerID: body.PeerId, Region: metacodec.DescriptorFromProto(body.GetDescriptor_())}
	}
	if body := pbEvent.GetLeaderTransfer(); body != nil {
		event.LeaderTransfer = &rootpkg.LeaderTransfer{RegionID: body.RegionId, FromPeerID: body.FromPeerId, ToPeerID: body.ToPeerId, TargetStoreID: body.TargetStoreId}
	}
	if body := pbEvent.GetPlacementPolicy(); body != nil {
		event.PlacementPolicy = &rootpkg.PlacementPolicy{Version: body.Version, Name: body.Name}
	}
	return event
}

func eventKindToPB(kind rootpkg.EventKind) metapb.RootEventKind {
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

func eventKindFromPB(kind metapb.RootEventKind) rootpkg.EventKind {
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
