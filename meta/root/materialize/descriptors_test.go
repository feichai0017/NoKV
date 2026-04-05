package materialize_test

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestSnapshotDescriptorEventsSorted(t *testing.T) {
	events := rootmaterialize.SnapshotDescriptorEvents(map[uint64]descriptor.Descriptor{
		7: testDescriptor(7, []byte("m"), []byte("z")),
		3: testDescriptor(3, []byte("a"), []byte("m")),
	})
	require.Len(t, events, 2)
	require.Equal(t, rootevent.KindRegionDescriptorPublished, events[0].Kind)
	require.Equal(t, uint64(3), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, uint64(7), events[1].RegionDescriptor.Descriptor.RegionID)
}

func TestApplyEventToSnapshotTracksPeerChangeStage(t *testing.T) {
	current := testDescriptor(11, []byte("a"), []byte("m"))
	current.RootEpoch = 5
	current.EnsureHash()

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{current.RegionID: current},
	}

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Contains(t, snapshot.PendingPeerChanges, target.RegionID)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.PeerAdded(target.RegionID, 2, 201, target))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingPeerChanges, target.RegionID)
}

func TestApplyEventToSnapshotTracksPendingSplitLifecycle(t *testing.T) {
	parent := testDescriptor(40, []byte("a"), []byte("z"))
	parent.RootEpoch = 5
	parent.EnsureHash()
	left := testDescriptor(40, []byte("a"), []byte("m"))
	right := testDescriptor(41, []byte("m"), []byte("z"))
	left.RootEpoch = 6
	right.RootEpoch = 6
	left.EnsureHash()
	right.EnsureHash()

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{parent.RegionID: parent},
	}

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.RegionSplitPlanned(parent.RegionID, []byte("m"), left, right))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Contains(t, snapshot.PendingRangeChanges, parent.RegionID)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.RegionSplitCommitted(parent.RegionID, []byte("m"), left, right))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingRangeChanges, parent.RegionID)
}

func TestApplyEventToSnapshotTracksPendingMergeLifecycle(t *testing.T) {
	left := testDescriptor(48, []byte("a"), []byte("m"))
	right := testDescriptor(49, []byte("m"), []byte("z"))
	merged := testDescriptor(50, []byte("a"), []byte("z"))
	left.RootEpoch = 5
	right.RootEpoch = 5
	merged.RootEpoch = 6
	left.EnsureHash()
	right.EnsureHash()
	merged.EnsureHash()

	snapshot := rootstate.Snapshot{
		State: rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{
			left.RegionID:  left,
			right.RegionID: right,
		},
	}

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.RegionMergePlanned(left.RegionID, right.RegionID, merged))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Contains(t, snapshot.PendingRangeChanges, merged.RegionID)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.RegionMerged(left.RegionID, right.RegionID, merged))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingRangeChanges, merged.RegionID)
}

func TestApplyEventToDescriptorsRestoresCancelledPeerChange(t *testing.T) {
	current := testDescriptor(81, []byte("a"), []byte("m"))
	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch++
	target.EnsureHash()

	descriptors := map[uint64]descriptor.Descriptor{target.RegionID: target}
	rootmaterialize.ApplyEventToDescriptors(descriptors, rootevent.PeerAdditionCancelled(target.RegionID, 2, 201, target, current))
	require.Equal(t, current, descriptors[current.RegionID])
}

func TestApplyEventToDescriptorsRestoresCancelledSplit(t *testing.T) {
	parent := testDescriptor(90, []byte("a"), []byte("z"))
	left := testDescriptor(90, []byte("a"), []byte("m"))
	right := testDescriptor(91, []byte("m"), []byte("z"))
	descriptors := map[uint64]descriptor.Descriptor{
		left.RegionID:  left,
		right.RegionID: right,
	}

	rootmaterialize.ApplyEventToDescriptors(descriptors, rootevent.RegionSplitCancelled(parent.RegionID, []byte("m"), left, right, parent))
	require.Equal(t, parent, descriptors[parent.RegionID])
	require.NotContains(t, descriptors, right.RegionID)
}

func TestApplyEventToDescriptorsRestoresCancelledMerge(t *testing.T) {
	baseLeft := testDescriptor(98, []byte("a"), []byte("m"))
	baseRight := testDescriptor(99, []byte("m"), []byte("z"))
	merged := testDescriptor(100, []byte("a"), []byte("z"))
	descriptors := map[uint64]descriptor.Descriptor{
		merged.RegionID: merged,
	}

	rootmaterialize.ApplyEventToDescriptors(descriptors, rootevent.RegionMergeCancelled(baseLeft.RegionID, baseRight.RegionID, merged, baseLeft, baseRight))
	require.Equal(t, baseLeft, descriptors[baseLeft.RegionID])
	require.Equal(t, baseRight, descriptors[baseRight.RegionID])
	require.NotContains(t, descriptors, merged.RegionID)
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
