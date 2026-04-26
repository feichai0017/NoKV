package state_test

import (
	"testing"

	eunomia "github.com/feichai0017/NoKV/coordinator/protocol/eunomia"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestApplyEventToStateAdvancesEpochsAndCursor(t *testing.T) {
	var st rootstate.State

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.StoreJoined(1))
	require.Equal(t, uint64(1), st.MembershipEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.LastCommitted)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))))
	require.Equal(t, uint64(1), st.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, st.LastCommitted)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 3}, rootevent.PeerAdditionPlanned(10, 2, 201, testDescriptor(10, []byte("a"), []byte("z"))))
	require.Equal(t, uint64(2), st.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, st.LastCommitted)
}

func TestApplyTenureToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.TenureGranted("c1", 1_000, 1, rootproto.MandateDefault, "pred", eunomia.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 0))

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, event)

	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.LastCommitted)
	require.Equal(t, "c1", st.Tenure.HolderID)
	require.Equal(t, int64(1_000), st.Tenure.ExpiresUnixNano)
	require.Equal(t, uint64(1), st.Tenure.Era)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.Tenure.IssuedAt)
	require.Equal(t, uint32(rootproto.MandateDefault), st.Tenure.Mandate)
	require.Equal(t, "pred", st.Tenure.LineageDigest)
	require.Equal(t, uint64(10), st.IDFence)
	require.Equal(t, uint64(20), st.TSOFence)
	require.True(t, st.Tenure.ActiveAt(999))
	require.False(t, st.Tenure.ActiveAt(1_000))
}

func TestApplyHandoverConfirmedToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.HandoverConfirmed("c1", 7, 8, "seal-digest")

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 2, Index: 9}, event)

	require.Equal(t, rootstate.Cursor{Term: 2, Index: 9}, st.LastCommitted)
	require.Equal(t, "c1", st.Handover.HolderID)
	require.Equal(t, uint64(7), st.Handover.LegacyEra)
	require.Equal(t, uint64(8), st.Handover.SuccessorEra)
	require.Equal(t, "seal-digest", st.Handover.LegacyDigest)
	require.Equal(t, rootproto.HandoverStageConfirmed, st.Handover.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 9}, st.Handover.ConfirmedAt)
}

func TestApplyHandoverClosedToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.HandoverClosed("c1", 7, 8, "seal-digest")

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 2, Index: 10}, event)

	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.LastCommitted)
	require.Equal(t, "c1", st.Handover.HolderID)
	require.Equal(t, uint64(8), st.Handover.SuccessorEra)
	require.Equal(t, uint64(7), st.Handover.LegacyEra)
	require.Equal(t, "seal-digest", st.Handover.LegacyDigest)
	require.Equal(t, rootproto.HandoverStageClosed, st.Handover.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.Handover.ClosedAt)
}

func TestApplyHandoverReattachToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.HandoverReattached("c1", 7, 8, "seal-digest")

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 2, Index: 10}, event)

	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.LastCommitted)
	require.Equal(t, "c1", st.Handover.HolderID)
	require.Equal(t, uint64(8), st.Handover.SuccessorEra)
	require.Equal(t, uint64(7), st.Handover.LegacyEra)
	require.Equal(t, "seal-digest", st.Handover.LegacyDigest)
	require.Equal(t, rootproto.HandoverStageReattached, st.Handover.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.Handover.ReattachedAt)
}

func TestApplyTenurePreservesIssuedAtForSameEra(t *testing.T) {
	var st rootstate.State
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.TenureGranted("c1", 1_000, 1, rootproto.MandateDefault, "pred", eunomia.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 0)))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.TenureGranted("c1", 2_000, 1, rootproto.MandateDefault, "", eunomia.Frontiers(rootstate.State{IDFence: 20, TSOFence: 30}, 0)))

	require.Equal(t, uint64(1), st.Tenure.Era)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.Tenure.IssuedAt)
	require.Equal(t, uint64(20), st.IDFence)
	require.Equal(t, uint64(30), st.TSOFence)
	require.Equal(t, "pred", st.Tenure.LineageDigest)
}

func TestApplyLegacyToStateDefaultsMandateAndClearsHandover(t *testing.T) {
	st := rootstate.State{
		Tenure: rootstate.Tenure{HolderID: "c1", Era: 7, Mandate: rootproto.MandateTSO},
		Handover: rootstate.Handover{
			HolderID:     "c1",
			LegacyEra:    6,
			SuccessorEra: 7,
			LegacyDigest: "old",
			Stage:        rootproto.HandoverStageConfirmed,
		},
	}
	cursor := rootstate.Cursor{Term: 2, Index: 8}
	rootstate.ApplyEventToState(&st, cursor, rootevent.TenureSealed("c1", 7, 0, eunomia.Frontiers(rootstate.State{TSOFence: 51}, 0)))

	require.Equal(t, rootstate.Legacy{
		HolderID:  "c1",
		Era:       7,
		Mandate:   rootproto.MandateTSO,
		Frontiers: eunomia.Frontiers(rootstate.State{TSOFence: 51}, 0),
		SealedAt:  cursor,
	}, st.Legacy)
	require.Equal(t, rootstate.Handover{}, st.Handover)
	require.Equal(t, cursor, st.LastCommitted)
}

func TestStateProtocolHelpers(t *testing.T) {
	witness := rootproto.NewMandateWitness(rootproto.MandateTSO, 8, 51)
	require.Equal(t, rootproto.MandateTSO, witness.Mandate)
	require.Equal(t, uint64(8), witness.Era)
	require.Equal(t, uint64(51), witness.ConsumedFrontier)

	seal := rootstate.Legacy{
		HolderID:  "c1",
		Era:       7,
		Mandate:   rootproto.MandateDefault,
		Frontiers: eunomia.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
	}
	require.Equal(t, uint64(20), seal.Frontiers.Frontier(rootproto.MandateAllocID))
	require.Equal(t, uint64(40), seal.Frontiers.Frontier(rootproto.MandateTSO))
	require.Equal(t, uint64(60), seal.Frontiers.Frontier(rootproto.MandateGetRegionByKey))
}

func TestCursorHelpers(t *testing.T) {
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, rootstate.NextCursor(rootstate.Cursor{}))
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 8}, rootstate.NextCursor(rootstate.Cursor{Term: 2, Index: 7}))
	require.True(t, rootstate.CursorAfter(rootstate.Cursor{Term: 1, Index: 2}, rootstate.Cursor{Term: 1, Index: 1}))
	require.True(t, rootstate.CursorAfter(rootstate.Cursor{Term: 2, Index: 1}, rootstate.Cursor{Term: 1, Index: 99}))
	require.False(t, rootstate.CursorAfter(rootstate.Cursor{Term: 1, Index: 1}, rootstate.Cursor{Term: 1, Index: 1}))
}

func TestCloneDescriptorsDetachesMapAndValues(t *testing.T) {
	in := map[uint64]descriptor.Descriptor{
		7: testDescriptor(7, []byte("m"), []byte("z")),
	}
	out := rootstate.CloneDescriptors(in)
	require.Equal(t, in[7].RegionID, out[7].RegionID)

	in[7].StartKey[0] = 'x'
	require.Equal(t, byte('m'), out[7].StartKey[0])
}

func TestCloneSnapshotDetachesAuthorityMapsAndPendingChanges(t *testing.T) {
	base := testDescriptor(7, []byte("a"), []byte("m"))
	target := testDescriptor(8, []byte("a"), []byte("m"))
	snapshot := rootstate.Snapshot{
		State: rootstate.State{ClusterEpoch: 3},
		Mounts: map[string]rootstate.MountRecord{
			"vol": {MountID: "vol", RootInode: 1, State: rootstate.MountStateActive},
		},
		Subtrees: map[string]rootstate.SubtreeAuthority{
			"vol/1": {SubtreeID: "vol/1", Mount: "vol", RootInode: 1, State: rootstate.SubtreeAuthorityActive},
		},
		Quotas: map[string]rootstate.QuotaFence{
			"vol/0": {SubjectID: "vol/0", Mount: "vol", Era: 1},
		},
		Descriptors: map[uint64]descriptor.Descriptor{7: base},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
			7: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 20, Base: base, Target: target},
		},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
			7: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: 7, BaseParent: base, Left: target},
		},
	}

	cloned := rootstate.CloneSnapshot(snapshot)
	peerChange := snapshot.PendingPeerChanges[7]
	peerChange.Target.StartKey[0] = 'x'
	snapshot.PendingPeerChanges[7] = peerChange
	rangeChange := snapshot.PendingRangeChanges[7]
	rangeChange.Left.StartKey[0] = 'x'
	snapshot.PendingRangeChanges[7] = rangeChange
	snapshot.Mounts["vol"] = rootstate.MountRecord{MountID: "vol", State: rootstate.MountStateRetired}
	snapshot.Subtrees["vol/1"] = rootstate.SubtreeAuthority{SubtreeID: "mutated"}
	snapshot.Quotas["vol/0"] = rootstate.QuotaFence{SubjectID: "mutated"}
	snapshot.PendingPeerChanges[7] = rootstate.PendingPeerChange{Base: testDescriptor(9, []byte("x"), []byte("z"))}
	snapshot.PendingRangeChanges[7] = rootstate.PendingRangeChange{BaseParent: testDescriptor(10, []byte("x"), []byte("z"))}

	require.Equal(t, rootstate.MountStateActive, cloned.Mounts["vol"].State)
	require.Equal(t, "vol/1", cloned.Subtrees["vol/1"].SubtreeID)
	require.Equal(t, uint64(1), cloned.Quotas["vol/0"].Era)
	require.Equal(t, uint64(7), cloned.PendingPeerChanges[7].Base.RegionID)
	require.Equal(t, uint64(7), cloned.PendingRangeChanges[7].BaseParent.RegionID)
	require.Equal(t, byte('a'), cloned.PendingPeerChanges[7].Target.StartKey[0])
	require.Equal(t, byte('a'), cloned.PendingRangeChanges[7].Left.StartKey[0])
}

func TestCloneEmptyMapsAndDescriptorRevision(t *testing.T) {
	require.Empty(t, rootstate.CloneMounts(nil))
	require.Nil(t, rootstate.CloneSubtreeAuthorities(nil))
	require.Nil(t, rootstate.CloneQuotaFences(nil))
	require.Empty(t, rootstate.ClonePendingPeerChanges(nil))
	require.Empty(t, rootstate.ClonePendingRangeChanges(nil))
	require.Zero(t, rootstate.MaxDescriptorRevision(nil))

	low := testDescriptor(1, []byte("a"), []byte("m"))
	low.RootEpoch = 3
	high := testDescriptor(2, []byte("m"), []byte("z"))
	high.RootEpoch = 9
	require.Equal(t, uint64(9), rootstate.MaxDescriptorRevision(map[uint64]descriptor.Descriptor{
		low.RegionID:  low,
		high.RegionID: high,
	}))
}

func TestApplyStoreMembershipEventsToSnapshot(t *testing.T) {
	var snapshot rootstate.Snapshot
	joinCursor := rootstate.Cursor{Term: 1, Index: 1}
	retireCursor := rootstate.Cursor{Term: 1, Index: 2}

	rootstate.ApplyEventToSnapshot(&snapshot, joinCursor, rootevent.StoreJoined(7))

	require.Equal(t, uint64(1), snapshot.State.MembershipEpoch)
	require.Equal(t, joinCursor, snapshot.State.LastCommitted)
	require.Equal(t, rootstate.StoreMembership{
		StoreID:  7,
		State:    rootstate.StoreMembershipActive,
		JoinedAt: joinCursor,
	}, snapshot.Stores[7])

	rootstate.ApplyEventToSnapshot(&snapshot, retireCursor, rootevent.StoreRetired(7))

	require.Equal(t, uint64(2), snapshot.State.MembershipEpoch)
	require.Equal(t, retireCursor, snapshot.State.LastCommitted)
	require.Equal(t, rootstate.StoreMembership{
		StoreID:   7,
		State:     rootstate.StoreMembershipRetired,
		JoinedAt:  joinCursor,
		RetiredAt: retireCursor,
	}, snapshot.Stores[7])
}

func TestApplySnapshotEpochPublishedToSnapshot(t *testing.T) {
	var snapshot rootstate.Snapshot
	cursor := rootstate.Cursor{Term: 2, Index: 7}
	event := rootevent.SnapshotEpochPublished("vol", 42, 100)

	rootstate.ApplyEventToSnapshot(&snapshot, cursor, event)

	id := rootevent.SnapshotEpochID("vol", 42, 100)
	require.Equal(t, cursor, snapshot.State.LastCommitted)
	require.Equal(t, rootstate.SnapshotEpoch{
		SnapshotID:  id,
		Mount:       "vol",
		RootInode:   42,
		ReadVersion: 100,
		PublishedAt: cursor,
	}, snapshot.SnapshotEpochs[id])

	retireCursor := rootstate.Cursor{Term: 2, Index: 8}
	rootstate.ApplyEventToSnapshot(&snapshot, retireCursor, rootevent.SnapshotEpochRetired("vol", 42, 100))
	require.Equal(t, retireCursor, snapshot.State.LastCommitted)
	require.NotContains(t, snapshot.SnapshotEpochs, id)
}

func TestCloneStoreMembershipsDetachesMap(t *testing.T) {
	in := map[uint64]rootstate.StoreMembership{
		7: {
			StoreID:  7,
			State:    rootstate.StoreMembershipActive,
			JoinedAt: rootstate.Cursor{Term: 1, Index: 1},
		},
	}

	out := rootstate.CloneStoreMemberships(in)
	in[7] = rootstate.StoreMembership{StoreID: 7, State: rootstate.StoreMembershipRetired}

	require.Equal(t, rootstate.StoreMembershipActive, out[7].State)
}

func TestCloneSnapshotEpochsDetachesMap(t *testing.T) {
	in := map[string]rootstate.SnapshotEpoch{
		"vol/7/9": {
			SnapshotID:  "vol/7/9",
			Mount:       "vol",
			RootInode:   7,
			ReadVersion: 9,
		},
	}

	out := rootstate.CloneSnapshotEpochs(in)
	in["vol/7/9"] = rootstate.SnapshotEpoch{SnapshotID: "mutated"}

	require.Equal(t, uint64(9), out["vol/7/9"].ReadVersion)
}

func testDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
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
