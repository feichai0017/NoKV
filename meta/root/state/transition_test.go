// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/meta/topology"
	"github.com/stretchr/testify/require"
)

func TestObserveRootEventLifecycle(t *testing.T) {
	target := testDescriptor(80, []byte("a"), []byte("z"))
	planned := rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)
	change, ok := rootstate.PendingPeerChangeFromEvent(planned)
	require.True(t, ok)

	lifecycle := rootstate.ObserveRootEventLifecycle(rootstate.Snapshot{
		Descriptors:        map[uint64]topology.Descriptor{target.RegionID: target},
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
		Descriptors:        map[uint64]topology.Descriptor{target.RegionID: target},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{target.RegionID: change},
	}

	decision, err := rootstate.EvaluateRootEventLifecycle(snapshot, peerPlanned)
	require.NoError(t, err)
	require.Equal(t, rootstate.RootEventLifecycleSkip, decision)

	decision, err = rootstate.EvaluateRootEventLifecycle(snapshot, peerApplied)
	require.NoError(t, err)
	require.Equal(t, rootstate.RootEventLifecycleApply, decision)
}

func TestTransitionIDFromEventDistinguishesPeerAddAndRemove(t *testing.T) {
	add := rootstate.TransitionIDFromEvent(rootevent.PeerAdditionPlanned(11, 2, 201, testDescriptor(11, nil, nil)))
	remove := rootstate.TransitionIDFromEvent(rootevent.PeerRemovalPlanned(11, 2, 201, testDescriptor(11, nil, nil)))

	require.Equal(t, "peer:11:add:2:201", add)
	require.Equal(t, "peer:11:remove:2:201", remove)
	require.NotEqual(t, add, remove)
}
