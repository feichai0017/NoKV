package state_test

import (
	"testing"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestApplyEventToStateAdvancesEpochsAndCursor(t *testing.T) {
	var st rootstate.State

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.StoreJoined(1, "s1"))
	require.Equal(t, uint64(1), st.MembershipEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.LastCommitted)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))))
	require.Equal(t, uint64(1), st.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, st.LastCommitted)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 3}, rootevent.PeerAdditionPlanned(10, 2, 201, testDescriptor(10, []byte("a"), []byte("z"))))
	require.Equal(t, uint64(2), st.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, st.LastCommitted)
}

func TestApplyCoordinatorLeaseToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.CoordinatorLeaseGranted("c1", 1_000, 1, rootstate.CoordinatorDutyMaskDefault, "pred", controlplane.Frontiers(10, 20, 0))

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, event)

	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.LastCommitted)
	require.Equal(t, "c1", st.CoordinatorLease.HolderID)
	require.Equal(t, int64(1_000), st.CoordinatorLease.ExpiresUnixNano)
	require.Equal(t, uint64(1), st.CoordinatorLease.CertGeneration)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.CoordinatorLease.IssuedCursor)
	require.Equal(t, uint32(rootstate.CoordinatorDutyMaskDefault), st.CoordinatorLease.DutyMask)
	require.Equal(t, "pred", st.CoordinatorLease.PredecessorDigest)
	require.Equal(t, uint64(10), st.IDFence)
	require.Equal(t, uint64(20), st.TSOFence)
	require.True(t, st.CoordinatorLease.ActiveAt(999))
	require.False(t, st.CoordinatorLease.ActiveAt(1_000))
}

func TestApplyCoordinatorClosureConfirmedToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.CoordinatorClosureConfirmed("c1", 7, 8, "seal-digest")

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 2, Index: 9}, event)

	require.Equal(t, rootstate.Cursor{Term: 2, Index: 9}, st.LastCommitted)
	require.Equal(t, "c1", st.CoordinatorClosure.HolderID)
	require.Equal(t, uint64(7), st.CoordinatorClosure.SealGeneration)
	require.Equal(t, uint64(8), st.CoordinatorClosure.SuccessorGeneration)
	require.Equal(t, "seal-digest", st.CoordinatorClosure.SealDigest)
	require.Equal(t, rootstate.CoordinatorClosureStageConfirmed, st.CoordinatorClosure.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 9}, st.CoordinatorClosure.ConfirmedAtCursor)
}

func TestApplyCoordinatorClosureClosedToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.CoordinatorClosureClosed("c1", 7, 8, "seal-digest")

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 2, Index: 10}, event)

	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.LastCommitted)
	require.Equal(t, "c1", st.CoordinatorClosure.HolderID)
	require.Equal(t, uint64(8), st.CoordinatorClosure.SuccessorGeneration)
	require.Equal(t, uint64(7), st.CoordinatorClosure.SealGeneration)
	require.Equal(t, "seal-digest", st.CoordinatorClosure.SealDigest)
	require.Equal(t, rootstate.CoordinatorClosureStageClosed, st.CoordinatorClosure.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.CoordinatorClosure.ClosedAtCursor)
}

func TestApplyCoordinatorClosureReattachToState(t *testing.T) {
	var st rootstate.State
	event := rootevent.CoordinatorClosureReattached("c1", 7, 8, "seal-digest")

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 2, Index: 10}, event)

	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.LastCommitted)
	require.Equal(t, "c1", st.CoordinatorClosure.HolderID)
	require.Equal(t, uint64(8), st.CoordinatorClosure.SuccessorGeneration)
	require.Equal(t, uint64(7), st.CoordinatorClosure.SealGeneration)
	require.Equal(t, "seal-digest", st.CoordinatorClosure.SealDigest)
	require.Equal(t, rootstate.CoordinatorClosureStageReattached, st.CoordinatorClosure.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, st.CoordinatorClosure.ReattachedAtCursor)
}

func TestApplyCoordinatorLeasePreservesIssuedCursorForSameGeneration(t *testing.T) {
	var st rootstate.State
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.CoordinatorLeaseGranted("c1", 1_000, 1, rootstate.CoordinatorDutyMaskDefault, "pred", controlplane.Frontiers(10, 20, 0)))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.CoordinatorLeaseGranted("c1", 2_000, 1, rootstate.CoordinatorDutyMaskDefault, "", controlplane.Frontiers(20, 30, 0)))

	require.Equal(t, uint64(1), st.CoordinatorLease.CertGeneration)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.CoordinatorLease.IssuedCursor)
	require.Equal(t, uint64(20), st.IDFence)
	require.Equal(t, uint64(30), st.TSOFence)
	require.Equal(t, "pred", st.CoordinatorLease.PredecessorDigest)
}

func TestStateProtocolHelpers(t *testing.T) {
	witness := rootstate.NewContinuationWitness(rootstate.CoordinatorDutyTSO, 8, 51)
	require.Equal(t, rootstate.CoordinatorDutyTSO, witness.DutyMask)
	require.Equal(t, "tso", witness.DutyName)
	require.Equal(t, uint64(8), witness.CertGeneration)
	require.Equal(t, uint64(51), witness.ConsumedFrontier)

	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 7,
		DutyMask:       rootstate.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(20, 40, 60),
	}
	required := rootstate.CoordinatorSealRequiredFrontiers(seal)
	require.Equal(t, uint64(20), required.Frontier(rootstate.CoordinatorDutyAllocID))
	require.Equal(t, uint64(40), required.Frontier(rootstate.CoordinatorDutyTSO))
	require.Equal(t, uint64(60), required.Frontier(rootstate.CoordinatorDutyGetRegionByKey))
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
