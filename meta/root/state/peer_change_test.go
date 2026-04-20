package state_test

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, rootstate.TransitionRetryNone, outcome.RetryClass)

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
	require.Equal(t, rootstate.TransitionRetryConflict, outcome.RetryClass)

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
