package state_test

import (
	"testing"

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

func TestPendingPeerChangeMatchesEvent(t *testing.T) {
	desc := testDescriptor(10, []byte("a"), []byte("z"))
	change, ok := rootstate.PendingPeerChangeFromEvent(rootevent.PeerAdditionPlanned(10, 2, 201, desc))
	require.True(t, ok)
	require.True(t, rootstate.PendingPeerChangeMatchesEvent(change, rootevent.PeerAdditionPlanned(10, 2, 201, desc)))
	require.True(t, rootstate.PendingPeerChangeMatchesEvent(change, rootevent.PeerAdded(10, 2, 201, desc)))
	require.False(t, rootstate.PendingPeerChangeMatchesEvent(change, rootevent.PeerRemoved(10, 2, 201, desc)))
}

func TestEvaluatePeerChangeLifecycle(t *testing.T) {
	target := testDescriptor(10, []byte("a"), []byte("z"))
	planned := rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)
	applied := rootevent.PeerAdded(target.RegionID, 2, 201, target)

	decision, err := rootstate.EvaluatePeerChangeLifecycle(nil, descriptor.Descriptor{}, false, planned)
	require.NoError(t, err)
	require.Equal(t, rootstate.PeerChangeLifecycleApply, decision)

	change, ok := rootstate.PendingPeerChangeFromEvent(planned)
	require.True(t, ok)
	snapshot := rootstate.Snapshot{
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{target.RegionID: change},
	}

	decision, err = rootstate.EvaluatePeerChangeLifecycle(snapshot.PendingPeerChanges, descriptor.Descriptor{}, false, planned)
	require.NoError(t, err)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, decision)

	decision, err = rootstate.EvaluatePeerChangeLifecycle(snapshot.PendingPeerChanges, descriptor.Descriptor{}, false, applied)
	require.NoError(t, err)
	require.Equal(t, rootstate.PeerChangeLifecycleApply, decision)

	conflicting := rootevent.PeerRemoved(target.RegionID, 3, 301, target)
	decision, err = rootstate.EvaluatePeerChangeLifecycle(snapshot.PendingPeerChanges, descriptor.Descriptor{}, false, conflicting)
	require.Error(t, err)
	require.Equal(t, rootstate.PeerChangeLifecycleApply, decision)

	decision, err = rootstate.EvaluatePeerChangeLifecycle(nil, target, true, applied)
	require.NoError(t, err)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, decision)
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
