package root

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestApplyEventToStateAdvancesEpochsAndCursor(t *testing.T) {
	var state State

	ApplyEventToState(&state, Cursor{Term: 1, Index: 1}, StoreJoined(1, "s1"))
	require.Equal(t, uint64(1), state.MembershipEpoch)
	require.Equal(t, Cursor{Term: 1, Index: 1}, state.LastCommitted)

	ApplyEventToState(&state, Cursor{Term: 1, Index: 2}, RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))))
	require.Equal(t, uint64(1), state.ClusterEpoch)
	require.Equal(t, Cursor{Term: 1, Index: 2}, state.LastCommitted)

	ApplyEventToState(&state, Cursor{Term: 1, Index: 3}, PlacementPolicyChanged("default", 9))
	require.Equal(t, uint64(9), state.PolicyVersion)
	require.Equal(t, Cursor{Term: 1, Index: 3}, state.LastCommitted)
}

func TestCursorHelpers(t *testing.T) {
	require.Equal(t, Cursor{Term: 1, Index: 1}, NextCursor(Cursor{}))
	require.Equal(t, Cursor{Term: 2, Index: 8}, NextCursor(Cursor{Term: 2, Index: 7}))
	require.True(t, CursorAfter(Cursor{Term: 1, Index: 2}, Cursor{Term: 1, Index: 1}))
	require.True(t, CursorAfter(Cursor{Term: 2, Index: 1}, Cursor{Term: 1, Index: 99}))
	require.False(t, CursorAfter(Cursor{Term: 1, Index: 1}, Cursor{Term: 1, Index: 1}))
}

func TestSnapshotDescriptorEventsSorted(t *testing.T) {
	events := SnapshotDescriptorEvents(map[uint64]descriptor.Descriptor{
		7: testDescriptor(7, []byte("m"), []byte("z")),
		3: testDescriptor(3, []byte("a"), []byte("m")),
	})
	require.Len(t, events, 2)
	require.Equal(t, EventKindRegionDescriptorPublished, events[0].Kind)
	require.Equal(t, uint64(3), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, uint64(7), events[1].RegionDescriptor.Descriptor.RegionID)
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
