package wire

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/meta/topology"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
)

func TestDescriptorRoundTripPreservesAndDetachesData(t *testing.T) {
	desc := testWireDescriptor(11, []byte("a"), []byte("m"))
	desc.Lineage = []topology.LineageRef{{
		RegionID: 7,
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 3},
		Hash:     []byte("lineage"),
		Kind:     topology.LineageKindSplitParent,
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
	require.Equal(t, topology.Descriptor{}, DescriptorFromProto(nil))

	pb.StartKey[0] = 'z'
	require.Equal(t, byte('a'), roundTrip.StartKey[0])
}

func TestRootStateProtocolAndCommandRoundTrip(t *testing.T) {
	issued := rootproto.Cursor{Term: 2, Index: 8}
	retired := rootproto.Cursor{Term: 2, Index: 9}
	grant := rootproto.AuthorityGrant{
		GrantID:         "grant-1",
		HolderID:        "coord-1",
		Era:             5,
		ExpiresUnixNano: 12345,
		IssuedAt:        issued,
		IssuedRootToken: rootproto.AuthorityRootToken{Term: 2, Index: 8},
		Duties: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10),
			rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{Term: 2, Index: 8}, 20, 3),
		},
	}
	retirement := rootproto.GrantRetirement{
		GrantID:   "grant-0",
		HolderID:  "coord-0",
		Era:       4,
		Mode:      rootproto.GrantRetirementExpiredBound,
		Bounds:    []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 9)},
		RetiredAt: retired,
	}
	inheritance := rootproto.GrantInheritance{
		PredecessorGrantID: "grant-0",
		SuccessorGrantID:   "grant-1",
		InheritedAt:        rootproto.Cursor{Term: 2, Index: 10},
	}
	perasGrant := testWirePerasAuthorityGrant()
	perasSeal := testWirePerasAuthoritySeal(perasGrant)
	grant.PredecessorRetirements = []rootproto.GrantRetirement{retirement}
	state := rootstate.State{
		ClusterEpoch:        7,
		MembershipEpoch:     3,
		LastCommitted:       rootproto.Cursor{Term: 2, Index: 9},
		IDFence:             100,
		TSOFence:            200,
		ActiveGrants:        []rootproto.AuthorityGrant{grant},
		RetiredGrants:       []rootproto.GrantRetirement{retirement},
		GrantInheritances:   []rootproto.GrantInheritance{inheritance},
		RetiredEraFloors:    []rootproto.AuthorityRetiredEraFloor{{DutyID: rootproto.DutyAllocID, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, RetiredEraFloor: 4}},
		ActivePerasGrants:   []rootproto.PerasAuthorityGrant{perasGrant},
		PerasAuthorityEpoch: perasGrant.EpochID,
		PerasAuthoritySeals: []rootproto.PerasAuthoritySeal{perasSeal},
	}

	require.Equal(t, state.LastCommitted, RootCursorFromProto(RootCursorToProto(state.LastCommitted)))
	require.Equal(t, rootproto.Cursor{}, RootCursorFromProto(nil))
	require.Equal(t, state, RootStateFromProto(RootStateToProto(state)))
	require.Equal(t, rootstate.State{}, RootStateFromProto(nil))

	require.Nil(t, RootAuthorityGrantToProto(rootproto.AuthorityGrant{}))
	require.Equal(t, grant, RootAuthorityGrantFromProto(RootAuthorityGrantToProto(grant)))
	require.Nil(t, RootGrantRetirementToProto(rootproto.GrantRetirement{}))
	require.Equal(t, retirement, RootGrantRetirementFromProto(RootGrantRetirementToProto(retirement)))
	require.Nil(t, RootGrantInheritanceToProto(rootproto.GrantInheritance{}))
	require.Equal(t, inheritance, RootGrantInheritanceFromProto(RootGrantInheritanceToProto(inheritance)))
	require.Nil(t, RootPerasAuthorityGrantToProto(rootproto.PerasAuthorityGrant{}))
	require.Equal(t, perasGrant, RootPerasAuthorityGrantFromProto(RootPerasAuthorityGrantToProto(perasGrant)))
	require.Nil(t, RootPerasAuthoritySealToProto(rootproto.PerasAuthoritySeal{}))
	require.Equal(t, perasSeal, RootPerasAuthoritySealFromProto(RootPerasAuthoritySealToProto(perasSeal)))

	protocolState := rootstate.EunomiaState{
		ActiveGrants:      state.ActiveGrants,
		RetiredGrants:     state.RetiredGrants,
		GrantInheritances: state.GrantInheritances,
		RetiredEraFloors:  state.RetiredEraFloors,
	}
	require.Equal(t, protocolState, RootEunomiaStateFromProto(RootEunomiaStateToProto(protocolState)))
	require.Equal(t, rootstate.EunomiaState{}, RootEunomiaStateFromProto(nil))

	grantCmd := rootproto.GrantCommand{
		Kind:                rootproto.GrantActIssue,
		HolderID:            "coord-1",
		GrantID:             "grant-1",
		ExpiresUnixNano:     23456,
		NowUnixNano:         12345,
		RequestedDuties:     grant.Duties,
		PredecessorGrantIDs: []string{"grant-0"},
	}
	require.Equal(t, grantCmd, RootGrantCommandFromProto(RootGrantCommandToProto(grantCmd)))
	require.Equal(t, rootproto.GrantCommand{}, RootGrantCommandFromProto(nil))

	perasCmd := rootproto.PerasAuthorityCommand{
		Kind:                 rootproto.PerasAuthorityActAcquire,
		HolderID:             perasGrant.HolderID,
		GrantID:              perasGrant.GrantID,
		Scope:                perasGrant.Scope,
		ExpiresUnixNano:      perasGrant.ExpiresUnixNano,
		NowUnixNano:          321,
		PredecessorDigest:    [32]byte{1, 2, 3},
		QuotaCreditBytes:     4096,
		QuotaCreditInodes:    128,
		SegmentRoot:          perasSeal.SegmentRoot,
		SegmentPayloadDigest: perasSeal.SegmentPayloadDigest,
		OperationCount:       perasSeal.OperationCount,
		EntryCount:           perasSeal.EntryCount,
	}
	require.Equal(t, perasCmd, RootPerasAuthorityCommandFromProto(RootPerasAuthorityCommandToProto(perasCmd)))
	require.Equal(t, rootproto.PerasAuthorityCommand{}, RootPerasAuthorityCommandFromProto(nil))

	cert := rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   []byte("sig"),
	}
	require.Equal(t, cert, RootGrantCertificateFromProto(RootGrantCertificateToProto(cert)))
	evidence := rootproto.AuthorityEvidence{
		Certificate:             cert,
		Usage:                   rootproto.AuthorityUsage{DutyID: rootproto.DutyAllocID, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 9}},
		ObservedRetirements:     []rootproto.GrantRetirement{retirement},
		ObservedRetiredEraFloor: 4,
	}
	require.Equal(t, evidence, RootAuthorityEvidenceFromProto(RootAuthorityEvidenceToProto(evidence)))

	require.Equal(t, metapb.RootGrantAct_ROOT_GRANT_ACT_UNSPECIFIED, RootGrantActToProto(rootproto.GrantActUnknown))
	require.Equal(t, rootproto.GrantActUnknown, RootGrantActFromProto(metapb.RootGrantAct_ROOT_GRANT_ACT_UNSPECIFIED))
	require.Equal(t, metapb.RootGrantRetirementMode_ROOT_GRANT_RETIREMENT_MODE_UNSPECIFIED, RootGrantRetirementModeToProto(rootproto.GrantRetirementUnspecified))
	require.Equal(t, rootproto.GrantRetirementUnspecified, RootGrantRetirementModeFromProto(metapb.RootGrantRetirementMode_ROOT_GRANT_RETIREMENT_MODE_UNSPECIFIED))
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
		Quotas: map[string]rootstate.QuotaFence{
			rootstate.QuotaFenceKey("vol", 0): {
				SubjectID:   rootstate.QuotaFenceKey("vol", 0),
				Mount:       "vol",
				LimitBytes:  4096,
				LimitInodes: 10,
				Era:         2,
				UpdatedAt:   rootproto.Cursor{Term: 2, Index: 11},
			},
		},
		Descriptors: map[uint64]topology.Descriptor{
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
	grant := rootproto.AuthorityGrant{
		GrantID:         "grant-1",
		HolderID:        "coord",
		Era:             7,
		ExpiresUnixNano: 123,
		Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
	}
	retirement := rootproto.GrantRetirement{
		GrantID:  grant.GrantID,
		HolderID: grant.HolderID,
		Era:      grant.Era,
		Mode:     rootproto.GrantRetirementSealedExact,
		Bounds:   grant.Duties,
	}

	events := []rootevent.Event{
		rootevent.StoreJoined(1),
		rootevent.IDAllocatorFenced(10),
		rootevent.GrantIssued(grant),
		rootevent.GrantSealed(retirement),
		rootevent.GrantInherited(rootproto.GrantInheritance{PredecessorGrantID: "grant-0", SuccessorGrantID: "grant-1"}),
		rootevent.PerasAuthorityGranted(testWirePerasAuthorityGrant()),
		rootevent.PerasAuthoritySealed(testWirePerasAuthoritySeal(testWirePerasAuthorityGrant())),
		rootevent.PerasAuthorityRetired(testWirePerasAuthorityGrant()),
		rootevent.SnapshotEpochPublished("vol", 1, 42, 99),
		rootevent.SnapshotEpochRetired("vol", 1, 42, 99),
		rootevent.MountRegistered("vol", 1, 1, 1),
		rootevent.MountRetired("vol"),
		rootevent.SubtreeAuthorityDeclared("vol", 1, "vol", 0, 10),
		rootevent.SubtreeHandoffStarted("vol", 1, 11),
		rootevent.SubtreeHandoffCompleted("vol", 1, 12),
		rootevent.QuotaFenceUpdated("vol", 1, 4096, 10, 2, 99),
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

	require.Nil(t, rootEventSnapshotEpochToProto(nil))
	require.Nil(t, rootEventMountToProto(nil))
	require.Nil(t, rootEventSubtreeAuthorityToProto(nil))
	require.Nil(t, rootEventQuotaFenceToProto(nil))
	require.Nil(t, rootEventSnapshotEpochFromProto(nil))
	require.Nil(t, rootEventMountFromProto(nil))
	require.Nil(t, rootEventSubtreeAuthorityFromProto(nil))
	require.Nil(t, rootEventQuotaFenceFromProto(nil))

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
		rootevent.KindGrantIssued,
		rootevent.KindGrantSealed,
		rootevent.KindGrantRetired,
		rootevent.KindGrantInherited,
		rootevent.KindPerasAuthorityGranted,
		rootevent.KindPerasAuthoritySealed,
		rootevent.KindPerasAuthorityRetired,
		rootevent.KindSnapshotEpochPublished,
		rootevent.KindSnapshotEpochRetired,
		rootevent.KindMountRegistered,
		rootevent.KindMountRetired,
		rootevent.KindSubtreeAuthorityDeclared,
		rootevent.KindSubtreeHandoffStarted,
		rootevent.KindSubtreeHandoffCompleted,
		rootevent.KindQuotaFenceUpdated,
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

func testWirePerasAuthorityGrant() rootproto.PerasAuthorityGrant {
	var predecessor [32]byte
	predecessor[0] = 7
	return rootproto.PerasAuthorityGrant{
		GrantID:  "peras-1",
		EpochID:  11,
		HolderID: "fsmeta-holder-a",
		Scope: rootproto.PerasAuthorityScope{
			MountID:    "vol",
			MountKeyID: 42,
			Buckets:    []uint16{1, 2},
			Parents:    []uint64{10},
			Inodes:     []uint64{20},
		},
		ExpiresUnixNano:   12345,
		PredecessorDigest: predecessor,
		QuotaCreditBytes:  4096,
		QuotaCreditInodes: 8,
	}
}

func testWirePerasAuthoritySeal(grant rootproto.PerasAuthorityGrant) rootproto.PerasAuthoritySeal {
	var root [32]byte
	var digest [32]byte
	root[0] = 9
	digest[0] = 8
	return rootproto.PerasAuthoritySeal{
		GrantID:              grant.GrantID,
		EpochID:              grant.EpochID,
		HolderID:             grant.HolderID,
		Scope:                grant.Scope,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		OperationCount:       10,
		EntryCount:           20,
		SealedUnixNano:       30,
		InstallRegionID:      7,
		InstallTerm:          3,
		InstallIndex:         99,
		InstallVersion:       1234,
	}
}

func testWireDescriptor(id uint64, start, end []byte) topology.Descriptor {
	desc := topology.Descriptor{
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
