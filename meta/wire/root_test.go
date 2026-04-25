package wire

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestDescriptorRoundTripPreservesAndDetachesData(t *testing.T) {
	desc := testWireDescriptor(11, []byte("a"), []byte("m"))
	desc.Lineage = []descriptor.LineageRef{{
		RegionID: 7,
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 3},
		Hash:     []byte("lineage"),
		Kind:     descriptor.LineageKindSplitParent,
	}}
	original := desc.Clone()

	pb := DescriptorToProto(desc)
	desc.StartKey[0] = 'x'
	desc.Peers[0].StoreID = 99
	desc.Lineage[0].Hash[0] = 'X'

	require.Equal(t, original.StartKey, pb.StartKey)
	require.Equal(t, original.Peers[0].StoreID, pb.Peers[0].StoreId)
	require.Equal(t, original.Lineage[0].Hash, pb.Lineage[0].Hash)

	roundTrip := DescriptorFromProto(pb)
	require.True(t, original.Equal(roundTrip))
	require.Equal(t, descriptor.Descriptor{}, DescriptorFromProto(nil))

	pb.StartKey[0] = 'z'
	require.Equal(t, byte('a'), roundTrip.StartKey[0])
}

func TestRootStateProtocolAndCommandRoundTrip(t *testing.T) {
	frontiers := rootproto.NewMandateFrontiers(
		rootproto.MandateFrontier{Mandate: rootproto.MandateAllocID, Frontier: 10},
		rootproto.MandateFrontier{Mandate: rootproto.MandateTSO, Frontier: 20},
	)
	state := rootstate.State{
		ClusterEpoch:    7,
		MembershipEpoch: 3,
		LastCommitted:   rootproto.Cursor{Term: 2, Index: 9},
		IDFence:         100,
		TSOFence:        200,
		Tenure: rootstate.Tenure{
			HolderID:        "coord-1",
			ExpiresUnixNano: 12345,
			Era:             5,
			IssuedAt:        rootproto.Cursor{Term: 2, Index: 8},
			Mandate:         rootproto.MandateDefault,
			LineageDigest:   "pred",
		},
		Legacy: rootstate.Legacy{
			HolderID:  "coord-1",
			Era:       5,
			Mandate:   rootproto.MandateAllocID | rootproto.MandateTSO,
			Frontiers: frontiers,
			SealedAt:  rootproto.Cursor{Term: 2, Index: 9},
		},
		Handover: rootstate.Handover{
			HolderID:     "coord-1",
			LegacyEra:    5,
			SuccessorEra: 6,
			LegacyDigest: "seal",
			Stage:        rootproto.HandoverStageClosed,
			ConfirmedAt:  rootproto.Cursor{Term: 2, Index: 10},
			ClosedAt:     rootproto.Cursor{Term: 2, Index: 11},
			ReattachedAt: rootproto.Cursor{Term: 2, Index: 12},
		},
	}

	require.Equal(t, state.LastCommitted, RootCursorFromProto(RootCursorToProto(state.LastCommitted)))
	require.Equal(t, rootproto.Cursor{}, RootCursorFromProto(nil))
	require.Equal(t, state, RootStateFromProto(RootStateToProto(state)))
	require.Equal(t, rootstate.State{}, RootStateFromProto(nil))

	require.Nil(t, RootTenureToProto(rootstate.Tenure{}))
	require.Nil(t, RootLegacyToProto(rootstate.Legacy{}))
	require.Nil(t, RootHandoverToProto(rootstate.Handover{}))
	require.Equal(t, state.Tenure, RootTenureFromProto(RootTenureToProto(state.Tenure)))
	require.Equal(t, state.Legacy, RootLegacyFromProto(RootLegacyToProto(state.Legacy)))
	require.Equal(t, state.Handover, RootHandoverFromProto(RootHandoverToProto(state.Handover)))

	require.Nil(t, RootMandateFrontiersToProto(rootproto.MandateFrontiers{}))
	filtered := RootMandateFrontiersFromProto([]*metapb.RootMandateFrontier{
		nil,
		{Mandate: 0, Frontier: 1},
		{Mandate: rootproto.MandateAllocID, Frontier: 10},
		{Mandate: rootproto.MandateTSO, Frontier: 20},
	})
	require.Equal(t, frontiers, filtered)

	protocolState := rootstate.SuccessionState{
		Tenure:   state.Tenure,
		Legacy:   state.Legacy,
		Handover: state.Handover,
	}
	require.Equal(t, protocolState, RootSuccessionStateFromProto(RootSuccessionStateToProto(protocolState)))
	require.Equal(t, rootstate.SuccessionState{}, RootSuccessionStateFromProto(nil))

	leaseCmd := rootproto.TenureCommand{
		Kind:               rootproto.TenureActRelease,
		HolderID:           "coord-1",
		ExpiresUnixNano:    23456,
		NowUnixNano:        12345,
		LineageDigest:      "pred",
		InheritedFrontiers: frontiers,
	}
	require.Equal(t, leaseCmd, RootTenureCommandFromProto(RootTenureCommandToProto(leaseCmd)))
	require.Equal(t, rootproto.TenureCommand{}, RootTenureCommandFromProto(nil))

	closureCmd := rootproto.HandoverCommand{
		Kind:        rootproto.HandoverActReattach,
		HolderID:    "coord-1",
		NowUnixNano: 999,
		Frontiers:   frontiers,
	}
	require.Equal(t, closureCmd, RootHandoverCommandFromProto(RootHandoverCommandToProto(closureCmd)))
	require.Equal(t, rootproto.HandoverCommand{}, RootHandoverCommandFromProto(nil))

	require.Equal(t, metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_UNSPECIFIED, rootHandoverStageToProto(rootproto.HandoverStageUnspecified))
	require.Equal(t, rootproto.HandoverStageUnspecified, rootHandoverStageFromProto(metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_UNSPECIFIED))
	require.Equal(t, metapb.RootTenureAct_ROOT_TENURE_ACT_UNSPECIFIED, rootTenureActToProto(rootproto.TenureActUnknown))
	require.Equal(t, rootproto.TenureActUnknown, rootTenureActFromProto(metapb.RootTenureAct_ROOT_TENURE_ACT_UNSPECIFIED))
	require.Equal(t, metapb.RootHandoverAct_ROOT_HANDOVER_ACT_UNSPECIFIED, rootHandoverActToProto(rootproto.HandoverActUnknown))
	require.Equal(t, rootproto.HandoverActUnknown, rootHandoverActFromProto(metapb.RootHandoverAct_ROOT_HANDOVER_ACT_UNSPECIFIED))
}

func TestRootSnapshotTailAndAllocatorRoundTrip(t *testing.T) {
	desc := testWireDescriptor(21, []byte("a"), []byte("m"))
	target := desc.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch++
	target.EnsureHash()

	left := testWireDescriptor(21, []byte("a"), []byte("f"))
	right := testWireDescriptor(22, []byte("f"), []byte("m"))
	snapshot := rootstate.Snapshot{
		State: rootstate.State{
			LastCommitted: rootproto.Cursor{Term: 3, Index: 9},
			IDFence:       50,
			TSOFence:      60,
		},
		Stores: map[uint64]rootstate.StoreMembership{
			1: {
				StoreID:   1,
				State:     rootstate.StoreMembershipRetired,
				JoinedAt:  rootproto.Cursor{Term: 1, Index: 1},
				RetiredAt: rootproto.Cursor{Term: 2, Index: 8},
			},
		},
		SnapshotEpochs: map[string]rootstate.SnapshotEpoch{
			"vol/42/99": {
				SnapshotID:  "vol/42/99",
				Mount:       "vol",
				RootInode:   42,
				ReadVersion: 99,
				PublishedAt: rootproto.Cursor{Term: 2, Index: 9},
			},
		},
		Mounts: map[string]rootstate.MountRecord{
			"vol": {
				MountID:       "vol",
				RootInode:     1,
				SchemaVersion: 1,
				State:         rootstate.MountStateActive,
				RegisteredAt:  rootproto.Cursor{Term: 2, Index: 10},
			},
		},
		Descriptors: map[uint64]descriptor.Descriptor{
			desc.RegionID: desc,
		},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
			desc.RegionID: {
				Kind:    rootstate.PendingPeerChangeAddition,
				StoreID: 2,
				PeerID:  201,
				Base:    desc,
				Target:  target,
			},
		},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
			left.RegionID: {
				Kind:           rootstate.PendingRangeChangeSplit,
				ParentRegionID: desc.RegionID,
				LeftRegionID:   left.RegionID,
				RightRegionID:  right.RegionID,
				BaseParent:     desc,
				Left:           left,
				Right:          right,
			},
		},
	}

	pbSnapshot := RootSnapshotToProto(snapshot, 77)
	roundTrip, tailOffset := RootSnapshotFromProto(pbSnapshot)
	require.Equal(t, uint64(77), tailOffset)
	require.Equal(t, snapshot, roundTrip)

	nilSnapshot, offset := RootSnapshotFromProto(nil)
	require.Empty(t, nilSnapshot.Descriptors)
	require.Zero(t, offset)

	observed := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: snapshot, TailOffset: 77},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 77,
			StartOffset:     78,
			EndOffset:       79,
			Records: []rootstorage.CommittedEvent{
				{
					Cursor: rootproto.Cursor{Term: 3, Index: 10},
					Event:  rootevent.RegionDescriptorPublished(target),
				},
			},
		},
	}
	checkpoint, tail := RootObservedToProto(observed)
	require.Equal(t, observed, RootObservedFromProto(checkpoint, tail))

	token := rootstorage.TailToken{Cursor: rootproto.Cursor{Term: 3, Index: 10}, Revision: 9}
	require.Equal(t, token, RootTailTokenFromProto(RootTailTokenToProto(token)))
	require.Equal(t, rootstorage.TailToken{}, RootTailTokenFromProto(nil))

	advance := observed.Advance(
		rootstorage.TailToken{Cursor: rootproto.Cursor{Term: 3, Index: 9}, Revision: 8},
		token,
	)
	after, current, pbCheckpoint, pbTail := RootTailAdvanceToObservedResponse(advance)
	require.Equal(t, advance, RootTailAdvanceFromProto(after, current, pbCheckpoint, pbTail))

	pbTail.Records = append(pbTail.Records, &metapb.RootCommittedEvent{
		Cursor: RootCursorToProto(rootproto.Cursor{Term: 3, Index: 11}),
		Event:  &metapb.RootEvent{Kind: metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED},
	})
	decodedTail := RootCommittedTailFromProto(pbTail)
	require.Len(t, decodedTail.Records, 1)

	fallback := RootFallbackObservedFromSnapshot(snapshot)
	require.Equal(t, snapshot, fallback.Checkpoint.Snapshot)
	require.Zero(t, fallback.Tail.StartOffset)
	require.Zero(t, fallback.Tail.EndOffset)

	require.Equal(t, metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_ID, RootAllocatorKindToProto(rootstate.AllocatorKindID))
	kind, ok := RootAllocatorKindFromProto(metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_TSO)
	require.True(t, ok)
	require.Equal(t, rootstate.AllocatorKindTSO, kind)
	kind, ok = RootAllocatorKindFromProto(metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_UNSPECIFIED)
	require.False(t, ok)
	require.Equal(t, rootstate.AllocatorKindUnknown, kind)
}

func TestRootEventRoundTripAndKindMappings(t *testing.T) {
	desc := testWireDescriptor(31, []byte("a"), []byte("m"))
	left := testWireDescriptor(31, []byte("a"), []byte("f"))
	right := testWireDescriptor(32, []byte("f"), []byte("m"))
	merged := testWireDescriptor(33, []byte("a"), []byte("m"))
	base := desc.Clone()
	frontiers := rootproto.NewMandateFrontiers(
		rootproto.MandateFrontier{Mandate: rootproto.MandateAllocID, Frontier: 10},
	)

	events := []rootevent.Event{
		rootevent.StoreJoined(1),
		rootevent.IDAllocatorFenced(10),
		rootevent.TenureGranted("coord", 123, 7, rootproto.MandateAllocID, "pred", frontiers),
		rootevent.TenureSealed("coord", 7, rootproto.MandateAllocID, frontiers),
		rootevent.HandoverClosed("coord", 7, 8, "seal"),
		rootevent.SnapshotEpochPublished("vol", 42, 99),
		rootevent.SnapshotEpochRetired("vol", 42, 99),
		rootevent.MountRegistered("vol", 1, 1),
		rootevent.MountRetired("vol"),
		rootevent.RegionDescriptorPublished(desc),
		rootevent.RegionTombstoned(desc.RegionID),
		rootevent.RegionSplitCancelled(desc.RegionID, []byte("f"), left, right, base),
		rootevent.RegionMergeCancelled(left.RegionID, right.RegionID, merged, left, right),
		rootevent.PeerAdditionCancelled(desc.RegionID, 2, 201, desc, base),
	}

	for _, event := range events {
		pb := RootEventToProto(event)
		got := RootEventFromProto(pb)
		require.Equal(t, event, got)
	}
	require.Equal(t, rootevent.Event{}, RootEventFromProto(nil))

	splitKey := []byte("f")
	event := rootevent.RegionSplitCommitted(desc.RegionID, splitKey, left, right)
	pb := RootEventToProto(event)
	splitKey[0] = 'x'
	event.RangeSplit.SplitKey[0] = 'x'
	got := RootEventFromProto(pb)
	require.Equal(t, []byte("f"), got.RangeSplit.SplitKey)

	require.Nil(t, rootEventTenureToProto(nil))
	require.Nil(t, rootEventLegacyToProto(nil))
	require.Nil(t, rootEventHandoverToProto(nil))
	require.Nil(t, rootEventSnapshotEpochToProto(nil))
	require.Nil(t, rootEventMountToProto(nil))
	require.Nil(t, rootEventTenureFromProto(nil))
	require.Nil(t, rootEventLegacyFromProto(nil))
	require.Nil(t, rootEventHandoverFromProto(nil))
	require.Nil(t, rootEventSnapshotEpochFromProto(nil))
	require.Nil(t, rootEventMountFromProto(nil))

	kinds := []rootevent.Kind{
		rootevent.KindStoreJoined,
		rootevent.KindStoreRetired,
		rootevent.KindIDAllocatorFenced,
		rootevent.KindTSOAllocatorFenced,
		rootevent.KindRegionBootstrap,
		rootevent.KindRegionDescriptorPublished,
		rootevent.KindRegionTombstoned,
		rootevent.KindRegionSplitPlanned,
		rootevent.KindRegionSplitCommitted,
		rootevent.KindRegionSplitCancelled,
		rootevent.KindRegionMergePlanned,
		rootevent.KindRegionMerged,
		rootevent.KindRegionMergeCancelled,
		rootevent.KindPeerAdditionPlanned,
		rootevent.KindPeerRemovalPlanned,
		rootevent.KindPeerAdded,
		rootevent.KindPeerRemoved,
		rootevent.KindPeerAdditionCancelled,
		rootevent.KindPeerRemovalCancelled,
		rootevent.KindTenure,
		rootevent.KindLegacy,
		rootevent.KindHandover,
		rootevent.KindSnapshotEpochPublished,
		rootevent.KindSnapshotEpochRetired,
		rootevent.KindMountRegistered,
		rootevent.KindMountRetired,
	}
	for _, kind := range kinds {
		require.Equal(t, kind, rootEventKindFromProto(rootEventKindToProto(kind)))
	}
	require.Equal(t, metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED, rootEventKindToProto(rootevent.KindUnknown))
	require.Equal(t, rootevent.KindUnknown, rootEventKindFromProto(metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED))

	require.Equal(t, rootstate.PendingPeerChangeAddition, rootPendingPeerChangeKindFromProto(rootPendingPeerChangeKindToProto(rootstate.PendingPeerChangeAddition)))
	require.Equal(t, rootstate.PendingPeerChangeUnknown, rootPendingPeerChangeKindFromProto(metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_UNSPECIFIED))
	require.Equal(t, rootstate.PendingRangeChangeMerge, rootPendingRangeChangeKindFromProto(rootPendingRangeChangeKindToProto(rootstate.PendingRangeChangeMerge)))
	require.Equal(t, rootstate.PendingRangeChangeUnknown, rootPendingRangeChangeKindFromProto(metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_UNSPECIFIED))
}

func testWireDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: id*10 + 1}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
