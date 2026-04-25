package state_test

import (
	"testing"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
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
	event := rootevent.TenureGranted("c1", 1_000, 1, rootproto.MandateDefault, "pred", succession.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 0))

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
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.TenureGranted("c1", 1_000, 1, rootproto.MandateDefault, "pred", succession.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 0)))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.TenureGranted("c1", 2_000, 1, rootproto.MandateDefault, "", succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 30}, 0)))

	require.Equal(t, uint64(1), st.Tenure.Era)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.Tenure.IssuedAt)
	require.Equal(t, uint64(20), st.IDFence)
	require.Equal(t, uint64(30), st.TSOFence)
	require.Equal(t, "pred", st.Tenure.LineageDigest)
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
		Frontiers: succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
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
