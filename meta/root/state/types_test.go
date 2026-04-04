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

func TestPendingRangeChangeMatchesEvent(t *testing.T) {
	left := testDescriptor(20, []byte("a"), []byte("m"))
	right := testDescriptor(21, []byte("m"), []byte("z"))
	merged := testDescriptor(20, []byte("a"), []byte("z"))

	_, split, ok := rootstate.PendingRangeChangeFromEvent(rootevent.RegionSplitPlanned(10, []byte("m"), left, right))
	require.True(t, ok)
	require.True(t, rootstate.PendingRangeChangeMatchesEvent(split, rootevent.RegionSplitPlanned(10, []byte("m"), left, right)))
	require.True(t, rootstate.PendingRangeChangeMatchesEvent(split, rootevent.RegionSplitCommitted(10, []byte("m"), left, right)))
	require.False(t, rootstate.PendingRangeChangeMatchesEvent(split, rootevent.RegionMerged(20, 21, merged)))

	_, merge, ok := rootstate.PendingRangeChangeFromEvent(rootevent.RegionMergePlanned(20, 21, merged))
	require.True(t, ok)
	require.True(t, rootstate.PendingRangeChangeMatchesEvent(merge, rootevent.RegionMergePlanned(20, 21, merged)))
	require.True(t, rootstate.PendingRangeChangeMatchesEvent(merge, rootevent.RegionMerged(20, 21, merged)))
}

func TestEvaluateRangeChangeLifecycle(t *testing.T) {
	left := testDescriptor(30, []byte("a"), []byte("m"))
	right := testDescriptor(31, []byte("m"), []byte("z"))
	splitPlanned := rootevent.RegionSplitPlanned(29, []byte("m"), left, right)
	splitCommitted := rootevent.RegionSplitCommitted(29, []byte("m"), left, right)

	decision, err := rootstate.EvaluateRangeChangeLifecycle(nil, nil, splitPlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, decision)

	key, pending, ok := rootstate.PendingRangeChangeFromEvent(splitPlanned)
	require.True(t, ok)
	pendingMap := map[uint64]rootstate.PendingRangeChange{key: pending}

	decision, err = rootstate.EvaluateRangeChangeLifecycle(pendingMap, nil, splitPlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, decision)

	decision, err = rootstate.EvaluateRangeChangeLifecycle(pendingMap, nil, splitCommitted)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, decision)

	conflictingRight := right.Clone()
	conflictingRight.EndKey = []byte("zz")
	conflictingRight.EnsureHash()
	conflicting := rootevent.RegionSplitCommitted(29, []byte("m"), left, conflictingRight)
	decision, err = rootstate.EvaluateRangeChangeLifecycle(pendingMap, nil, conflicting)
	require.Error(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, decision)

	descriptors := map[uint64]descriptor.Descriptor{
		left.RegionID:  left,
		right.RegionID: right,
	}
	decision, err = rootstate.EvaluateRangeChangeLifecycle(nil, descriptors, splitCommitted)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, decision)
}

func TestApplyRangeChangeToSnapshot(t *testing.T) {
	parent := testDescriptor(40, []byte("a"), []byte("z"))
	left := testDescriptor(40, []byte("a"), []byte("m"))
	right := testDescriptor(41, []byte("m"), []byte("z"))

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{parent.RegionID: parent},
	}

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&snapshot, rootevent.RegionSplitPlanned(parent.RegionID, []byte("m"), left, right)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Contains(t, snapshot.PendingRangeChanges, parent.RegionID)

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&snapshot, rootevent.RegionSplitCommitted(parent.RegionID, []byte("m"), left, right)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingRangeChanges, parent.RegionID)
}

func TestApplyPeerChangeToSnapshot(t *testing.T) {
	current := testDescriptor(11, []byte("a"), []byte("m"))
	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch++
	target.EnsureHash()

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{current.RegionID: current},
	}

	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Contains(t, snapshot.PendingPeerChanges, target.RegionID)

	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdded(target.RegionID, 2, 201, target)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingPeerChanges, target.RegionID)
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
