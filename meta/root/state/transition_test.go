package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

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

func TestTransitionIDFromEventDistinguishesPeerAddAndRemove(t *testing.T) {
	add := rootstate.TransitionIDFromEvent(rootevent.PeerAdditionPlanned(11, 2, 201, testDescriptor(11, nil, nil)))
	remove := rootstate.TransitionIDFromEvent(rootevent.PeerRemovalPlanned(11, 2, 201, testDescriptor(11, nil, nil)))

	require.Equal(t, "peer:11:add:2:201", add)
	require.Equal(t, "peer:11:remove:2:201", remove)
	require.NotEqual(t, add, remove)
}
