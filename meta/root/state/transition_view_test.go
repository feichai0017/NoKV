package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

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
