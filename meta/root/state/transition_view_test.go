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

func TestBuildTransitionEntries(t *testing.T) {
	peerTarget := testDescriptor(80, []byte("a"), []byte("z"))
	peerPlanned := rootevent.PeerAdditionPlanned(peerTarget.RegionID, 2, 201, peerTarget)
	peerChange, ok := rootstate.PendingPeerChangeFromEvent(peerPlanned)
	require.True(t, ok)

	entries := rootstate.BuildTransitionEntries(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{
			peerTarget.RegionID: peerTarget,
		},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{peerTarget.RegionID: peerChange},
	})
	require.Len(t, entries, 1)

	require.Equal(t, rootstate.TransitionKindPeerChange, entries[0].Kind)
	require.Equal(t, peerTarget.RegionID, entries[0].Key)
	require.Equal(t, rootstate.TransitionStatusPending, entries[0].Status)
	require.NotNil(t, entries[0].PeerChange)
}
