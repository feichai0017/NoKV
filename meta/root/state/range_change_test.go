package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, rootstate.TransitionRetryNone, outcome.RetryClass)

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
	require.Equal(t, rootstate.TransitionRetryConflict, outcome.RetryClass)

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
