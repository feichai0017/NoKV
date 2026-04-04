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

	rootmaterialize.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Equal(t, rootstate.PendingPeerChangeStagePlanned, snapshot.PendingPeerChanges[target.RegionID].Stage)

	rootmaterialize.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.PeerAdded(target.RegionID, 2, 201, target))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Equal(t, rootstate.PendingPeerChangeStageApplied, snapshot.PendingPeerChanges[target.RegionID].Stage)
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
