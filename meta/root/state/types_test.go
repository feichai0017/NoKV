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

	decision, err = rootstate.EvaluatePeerChangeLifecycle(nil, target, true, planned)
	require.NoError(t, err)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, decision)

	newer := target.Clone()
	newer.RootEpoch = target.RootEpoch + 1
	newer.EnsureHash()
	decision, err = rootstate.EvaluatePeerChangeLifecycle(nil, newer, true, applied)
	require.Error(t, err)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, decision)
}

func TestObservePeerChangeCompletion(t *testing.T) {
	target := testDescriptor(16, []byte("a"), []byte("z"))
	planned := rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)

	completion := rootstate.ObservePeerChangeCompletion(nil, descriptor.Descriptor{}, false, planned)
	require.Equal(t, rootstate.PeerChangeCompletionOpen, completion.State)
	require.True(t, completion.Open())

	change, ok := rootstate.PendingPeerChangeFromEvent(planned)
	require.True(t, ok)
	completion = rootstate.ObservePeerChangeCompletion(
		map[uint64]rootstate.PendingPeerChange{target.RegionID: change},
		descriptor.Descriptor{},
		false,
		planned,
	)
	require.Equal(t, rootstate.PeerChangeCompletionPending, completion.State)
	require.True(t, completion.PendingState())
	require.Equal(t, change, completion.Pending)

	completion = rootstate.ObservePeerChangeCompletion(nil, target, true, planned)
	require.Equal(t, rootstate.PeerChangeCompletionCompleted, completion.State)
	require.True(t, completion.Completed())
}

func TestObservePeerChangeLifecycle(t *testing.T) {
	target := testDescriptor(17, []byte("a"), []byte("z"))
	planned := rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)
	applied := rootevent.PeerAdded(target.RegionID, 2, 201, target)
	conflicting := rootevent.PeerRemoved(target.RegionID, 3, 301, target)

	outcome := rootstate.ObservePeerChangeLifecycle(nil, descriptor.Descriptor{}, false, planned)
	require.Equal(t, rootstate.PeerChangeLifecycleApply, outcome.Decision)
	require.True(t, outcome.Completion.Open())
	require.Equal(t, rootstate.TransitionStatusOpen, outcome.Status)
	require.False(t, outcome.Retryable())

	change, ok := rootstate.PendingPeerChangeFromEvent(planned)
	require.True(t, ok)
	outcome = rootstate.ObservePeerChangeLifecycle(
		map[uint64]rootstate.PendingPeerChange{target.RegionID: change},
		descriptor.Descriptor{},
		false,
		conflicting,
	)
	require.Equal(t, rootstate.PeerChangeLifecycleApply, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusConflict, outcome.Status)
	require.True(t, outcome.Retryable())

	outcome = rootstate.ObservePeerChangeLifecycle(nil, target, true, applied)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, outcome.Decision)
	require.True(t, outcome.Completion.Completed())
	require.Equal(t, rootstate.TransitionStatusCompleted, outcome.Status)

	newer := target.Clone()
	newer.RootEpoch = target.RootEpoch + 1
	newer.EnsureHash()
	outcome = rootstate.ObservePeerChangeLifecycle(nil, newer, true, planned)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusSuperseded, outcome.Status)

	outcome = rootstate.ObservePeerChangeLifecycle(nil, newer, true, applied)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusAborted, outcome.Status)
}

func TestObservePeerChangeCancelLifecycle(t *testing.T) {
	current := testDescriptor(171, []byte("a"), []byte("z"))
	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch++
	target.EnsureHash()

	pending := rootstate.PendingPeerChange{
		Kind:    rootstate.PendingPeerChangeAddition,
		StoreID: 2,
		PeerID:  201,
		Base:    current,
		Target:  target,
	}

	outcome := rootstate.ObservePeerChangeLifecycle(
		map[uint64]rootstate.PendingPeerChange{target.RegionID: pending},
		target,
		true,
		rootevent.PeerAdditionCancelled(target.RegionID, 2, 201, target, current),
	)
	require.Equal(t, rootstate.PeerChangeLifecycleApply, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusPending, outcome.Status)

	outcome = rootstate.ObservePeerChangeLifecycle(
		nil,
		current,
		true,
		rootevent.PeerAdditionCancelled(target.RegionID, 2, 201, target, current),
	)
	require.Equal(t, rootstate.PeerChangeLifecycleSkip, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusCancelled, outcome.Status)
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

	decision, err = rootstate.EvaluateRangeChangeLifecycle(nil, descriptors, splitPlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, decision)

	merged := testDescriptor(50, []byte("a"), []byte("z"))
	mergePlanned := rootevent.RegionMergePlanned(48, 49, merged)
	mergeCommitted := rootevent.RegionMerged(48, 49, merged)

	decision, err = rootstate.EvaluateRangeChangeLifecycle(nil, nil, mergePlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, decision)

	key, pending, ok = rootstate.PendingRangeChangeFromEvent(mergePlanned)
	require.True(t, ok)
	pendingMap = map[uint64]rootstate.PendingRangeChange{key: pending}

	decision, err = rootstate.EvaluateRangeChangeLifecycle(pendingMap, nil, mergePlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, decision)

	decision, err = rootstate.EvaluateRangeChangeLifecycle(pendingMap, nil, mergeCommitted)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, decision)

	mergeDescriptors := map[uint64]descriptor.Descriptor{
		merged.RegionID: merged,
	}
	decision, err = rootstate.EvaluateRangeChangeLifecycle(nil, mergeDescriptors, mergeCommitted)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, decision)

	decision, err = rootstate.EvaluateRangeChangeLifecycle(nil, mergeDescriptors, mergePlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, decision)
}

func TestObserveRangeChangeCompletion(t *testing.T) {
	left := testDescriptor(70, []byte("a"), []byte("m"))
	right := testDescriptor(71, []byte("m"), []byte("z"))
	splitPlanned := rootevent.RegionSplitPlanned(69, []byte("m"), left, right)

	completion := rootstate.ObserveRangeChangeCompletion(nil, nil, splitPlanned)
	require.Equal(t, rootstate.RangeChangeCompletionOpen, completion.State)
	require.True(t, completion.Open())
	require.True(t, completion.NeedsEpochAdvance(false))

	key, pending, ok := rootstate.PendingRangeChangeFromEvent(splitPlanned)
	require.True(t, ok)
	completion = rootstate.ObserveRangeChangeCompletion(
		map[uint64]rootstate.PendingRangeChange{key: pending},
		nil,
		splitPlanned,
	)
	require.Equal(t, rootstate.RangeChangeCompletionPending, completion.State)
	require.True(t, completion.PendingState())
	require.Equal(t, key, completion.Key)

	completion = rootstate.ObserveRangeChangeCompletion(
		nil,
		map[uint64]descriptor.Descriptor{
			left.RegionID:  left,
			right.RegionID: right,
		},
		splitPlanned,
	)
	require.Equal(t, rootstate.RangeChangeCompletionCompleted, completion.State)
	require.True(t, completion.Completed())
	require.True(t, completion.NeedsEpochAdvance(true))
}

func TestObserveRangeChangeLifecycle(t *testing.T) {
	left := testDescriptor(72, []byte("a"), []byte("m"))
	right := testDescriptor(73, []byte("m"), []byte("z"))
	splitPlanned := rootevent.RegionSplitPlanned(71, []byte("m"), left, right)
	splitCommitted := rootevent.RegionSplitCommitted(71, []byte("m"), left, right)

	outcome := rootstate.ObserveRangeChangeLifecycle(nil, nil, splitPlanned)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, outcome.Decision)
	require.True(t, outcome.Completion.Open())
	require.Equal(t, rootstate.TransitionStatusOpen, outcome.Status)
	require.False(t, outcome.Retryable())

	key, pending, ok := rootstate.PendingRangeChangeFromEvent(splitPlanned)
	require.True(t, ok)
	conflictingRight := right.Clone()
	conflictingRight.EndKey = []byte("zz")
	conflictingRight.EnsureHash()
	conflicting := rootevent.RegionSplitCommitted(71, []byte("m"), left, conflictingRight)
	outcome = rootstate.ObserveRangeChangeLifecycle(
		map[uint64]rootstate.PendingRangeChange{key: pending},
		nil,
		conflicting,
	)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusConflict, outcome.Status)
	require.True(t, outcome.Retryable())

	outcome = rootstate.ObserveRangeChangeLifecycle(
		nil,
		map[uint64]descriptor.Descriptor{
			left.RegionID:  left,
			right.RegionID: right,
		},
		splitCommitted,
	)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, outcome.Decision)
	require.True(t, outcome.Completion.Completed())
	require.Equal(t, rootstate.TransitionStatusCompleted, outcome.Status)

	newerLeft := left.Clone()
	newerLeft.RootEpoch++
	newerLeft.EndKey = []byte("l")
	newerLeft.EnsureHash()
	outcome = rootstate.ObserveRangeChangeLifecycle(
		nil,
		map[uint64]descriptor.Descriptor{newerLeft.RegionID: newerLeft},
		splitPlanned,
	)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusSuperseded, outcome.Status)

	outcome = rootstate.ObserveRangeChangeLifecycle(
		nil,
		map[uint64]descriptor.Descriptor{newerLeft.RegionID: newerLeft},
		splitCommitted,
	)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusAborted, outcome.Status)
}

func TestObserveRangeChangeCancelLifecycle(t *testing.T) {
	parent := testDescriptor(260, []byte("a"), []byte("z"))
	left := testDescriptor(260, []byte("a"), []byte("m"))
	right := testDescriptor(261, []byte("m"), []byte("z"))
	left.RootEpoch = parent.RootEpoch + 1
	right.RootEpoch = parent.RootEpoch + 1
	left.EnsureHash()
	right.EnsureHash()

	key := parent.RegionID
	pending := rootstate.PendingRangeChange{
		Kind:           rootstate.PendingRangeChangeSplit,
		ParentRegionID: parent.RegionID,
		LeftRegionID:   left.RegionID,
		RightRegionID:  right.RegionID,
		BaseParent:     parent,
		Left:           left,
		Right:          right,
	}

	outcome := rootstate.ObserveRangeChangeLifecycle(
		map[uint64]rootstate.PendingRangeChange{key: pending},
		map[uint64]descriptor.Descriptor{
			left.RegionID:  left,
			right.RegionID: right,
		},
		rootevent.RegionSplitCancelled(parent.RegionID, []byte("m"), left, right, parent),
	)
	require.Equal(t, rootstate.RangeChangeLifecycleApply, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusPending, outcome.Status)

	outcome = rootstate.ObserveRangeChangeLifecycle(
		nil,
		map[uint64]descriptor.Descriptor{parent.RegionID: parent},
		rootevent.RegionSplitCancelled(parent.RegionID, []byte("m"), left, right, parent),
	)
	require.Equal(t, rootstate.RangeChangeLifecycleSkip, outcome.Decision)
	require.Equal(t, rootstate.TransitionStatusCancelled, outcome.Status)
}

func TestObserveRootEventLifecycle(t *testing.T) {
	target := testDescriptor(80, []byte("a"), []byte("z"))
	planned := rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)
	change, ok := rootstate.PendingPeerChangeFromEvent(planned)
	require.True(t, ok)

	lifecycle := rootstate.ObserveRootEventLifecycle(rootstate.Snapshot{
		Descriptors:        map[uint64]descriptor.Descriptor{target.RegionID: target},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{target.RegionID: change},
	}, planned)
	require.Equal(t, rootstate.TransitionKindPeerChange, lifecycle.Kind)
	require.Equal(t, target.RegionID, lifecycle.Key)
	require.Equal(t, rootstate.TransitionStatusPending, lifecycle.Status)
	require.Equal(t, rootstate.RootEventLifecycleSkip, lifecycle.Decision)
}

func TestBuildTransitionEntries(t *testing.T) {
	peerTarget := testDescriptor(80, []byte("a"), []byte("z"))
	peerPlanned := rootevent.PeerAdditionPlanned(peerTarget.RegionID, 2, 201, peerTarget)
	peerChange, ok := rootstate.PendingPeerChangeFromEvent(peerPlanned)
	require.True(t, ok)

	left := testDescriptor(190, []byte("a"), []byte("m"))
	right := testDescriptor(191, []byte("m"), []byte("z"))
	merged := testDescriptor(200, []byte("a"), []byte("z"))
	mergePlanned := rootevent.RegionMergePlanned(left.RegionID, right.RegionID, merged)
	rangeKey, rangeChange, ok := rootstate.PendingRangeChangeFromEvent(mergePlanned)
	require.True(t, ok)

	entries := rootstate.BuildTransitionEntries(rootstate.Snapshot{
		Descriptors: map[uint64]descriptor.Descriptor{
			peerTarget.RegionID: peerTarget,
			left.RegionID:       left,
			right.RegionID:      right,
		},
		PendingPeerChanges:  map[uint64]rootstate.PendingPeerChange{peerTarget.RegionID: peerChange},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{rangeKey: rangeChange},
	})
	require.Len(t, entries, 2)

	require.Equal(t, rootstate.TransitionKindPeerChange, entries[0].Kind)
	require.Equal(t, peerTarget.RegionID, entries[0].Key)
	require.Equal(t, rootstate.TransitionStatusPending, entries[0].Status)
	require.NotNil(t, entries[0].PeerChange)

	require.Equal(t, rootstate.TransitionKindRangeChange, entries[1].Kind)
	require.Equal(t, merged.RegionID, entries[1].Key)
	require.Equal(t, rootstate.TransitionStatusPending, entries[1].Status)
	require.NotNil(t, entries[1].RangeChange)
}

func TestEvaluateRootEventLifecycle(t *testing.T) {
	target := testDescriptor(50, []byte("a"), []byte("z"))
	peerPlanned := rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)
	peerApplied := rootevent.PeerAdded(target.RegionID, 2, 201, target)

	change, ok := rootstate.PendingPeerChangeFromEvent(peerPlanned)
	require.True(t, ok)
	snapshot := rootstate.Snapshot{
		Descriptors:        map[uint64]descriptor.Descriptor{target.RegionID: target},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{target.RegionID: change},
	}

	decision, err := rootstate.EvaluateRootEventLifecycle(snapshot, peerPlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RootEventLifecycleSkip, decision)

	decision, err = rootstate.EvaluateRootEventLifecycle(snapshot, peerApplied)
	require.NoError(t, err)
	require.Equal(t, rootstate.RootEventLifecycleApply, decision)

	left := testDescriptor(60, []byte("a"), []byte("m"))
	right := testDescriptor(61, []byte("m"), []byte("z"))
	splitPlanned := rootevent.RegionSplitPlanned(59, []byte("m"), left, right)
	key, pending, ok := rootstate.PendingRangeChangeFromEvent(splitPlanned)
	require.True(t, ok)

	rangeSnapshot := rootstate.Snapshot{
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{key: pending},
	}
	decision, err = rootstate.EvaluateRootEventLifecycle(rangeSnapshot, splitPlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RootEventLifecycleSkip, decision)
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

	merged := testDescriptor(50, []byte("a"), []byte("z"))
	leftMerge := testDescriptor(48, []byte("a"), []byte("m"))
	rightMerge := testDescriptor(49, []byte("m"), []byte("z"))
	mergeSnapshot := rootstate.Snapshot{
		State: rootstate.State{ClusterEpoch: 7},
		Descriptors: map[uint64]descriptor.Descriptor{
			leftMerge.RegionID:  leftMerge,
			rightMerge.RegionID: rightMerge,
		},
	}

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMergePlanned(leftMerge.RegionID, rightMerge.RegionID, merged)))
	require.Equal(t, uint64(8), mergeSnapshot.State.ClusterEpoch)
	require.Contains(t, mergeSnapshot.PendingRangeChanges, merged.RegionID)

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMerged(leftMerge.RegionID, rightMerge.RegionID, merged)))
	require.Equal(t, uint64(8), mergeSnapshot.State.ClusterEpoch)
	require.NotContains(t, mergeSnapshot.PendingRangeChanges, merged.RegionID)
}

func TestApplyRangeChangeCancelToSnapshot(t *testing.T) {
	parent := testDescriptor(140, []byte("a"), []byte("z"))
	left := testDescriptor(140, []byte("a"), []byte("m"))
	right := testDescriptor(141, []byte("m"), []byte("z"))
	left.RootEpoch = parent.RootEpoch + 1
	right.RootEpoch = parent.RootEpoch + 1
	left.EnsureHash()
	right.EnsureHash()

	splitSnapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 10},
		Descriptors: map[uint64]descriptor.Descriptor{parent.RegionID: parent},
	}
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&splitSnapshot, rootevent.RegionSplitPlanned(parent.RegionID, []byte("m"), left, right)))
	require.Equal(t, parent, splitSnapshot.PendingRangeChanges[parent.RegionID].BaseParent)
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&splitSnapshot, rootevent.RegionSplitCancelled(parent.RegionID, []byte("m"), left, right, parent)))
	require.Equal(t, parent, splitSnapshot.Descriptors[parent.RegionID])
	require.NotContains(t, splitSnapshot.Descriptors, right.RegionID)
	require.NotContains(t, splitSnapshot.PendingRangeChanges, parent.RegionID)

	baseLeft := testDescriptor(148, []byte("a"), []byte("m"))
	baseRight := testDescriptor(149, []byte("m"), []byte("z"))
	merged := testDescriptor(150, []byte("a"), []byte("z"))
	baseLeft.RootEpoch = 5
	baseRight.RootEpoch = 5
	merged.RootEpoch = 6
	baseLeft.EnsureHash()
	baseRight.EnsureHash()
	merged.EnsureHash()
	mergeSnapshot := rootstate.Snapshot{
		State: rootstate.State{ClusterEpoch: 20},
		Descriptors: map[uint64]descriptor.Descriptor{
			baseLeft.RegionID:  baseLeft,
			baseRight.RegionID: baseRight,
		},
	}
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMergePlanned(baseLeft.RegionID, baseRight.RegionID, merged)))
	require.Equal(t, baseLeft, mergeSnapshot.PendingRangeChanges[merged.RegionID].BaseLeft)
	require.Equal(t, baseRight, mergeSnapshot.PendingRangeChanges[merged.RegionID].BaseRight)
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMergeCancelled(baseLeft.RegionID, baseRight.RegionID, merged, baseLeft, baseRight)))
	require.Equal(t, baseLeft, mergeSnapshot.Descriptors[baseLeft.RegionID])
	require.Equal(t, baseRight, mergeSnapshot.Descriptors[baseRight.RegionID])
	require.NotContains(t, mergeSnapshot.Descriptors, merged.RegionID)
	require.NotContains(t, mergeSnapshot.PendingRangeChanges, merged.RegionID)
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
	require.Equal(t, current, snapshot.PendingPeerChanges[target.RegionID].Base)

	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdded(target.RegionID, 2, 201, target)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingPeerChanges, target.RegionID)
}

func TestApplyPeerChangeCancelToSnapshot(t *testing.T) {
	current := testDescriptor(111, []byte("a"), []byte("m"))
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
	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdditionCancelled(target.RegionID, 2, 201, target, current)))
	require.Equal(t, current, snapshot.Descriptors[current.RegionID])
	require.NotContains(t, snapshot.PendingPeerChanges, current.RegionID)
	require.Equal(t, uint64(7), snapshot.State.ClusterEpoch)
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
