package core

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterStoreHeartbeatAndSnapshot(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.UpsertStoreHeartbeat(StoreStats{StoreID: 1, RegionNum: 3}))
	require.NoError(t, c.UpsertStoreHeartbeat(StoreStats{StoreID: 2, RegionNum: 5}))

	snap := c.StoreSnapshot()
	require.Len(t, snap, 2)
	require.Equal(t, uint64(1), snap[0].StoreID)
	require.Equal(t, uint64(2), snap[1].StoreID)
	require.False(t, snap[0].UpdatedAt.IsZero())

	c.RemoveStore(1)
	snap = c.StoreSnapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(2), snap[0].StoreID)
}

func TestClusterRegionHeartbeatAndRouteLookup(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(1, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})))
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 1, ConfVersion: 1})))

	desc, ok := c.GetRegionDescriptorByKey([]byte("a"))
	require.True(t, ok)
	require.Equal(t, uint64(1), desc.RegionID)

	desc, ok = c.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(2), desc.RegionID)

	desc, ok = c.GetRegionDescriptorByKey([]byte("z"))
	require.True(t, ok)
	require.Equal(t, uint64(2), desc.RegionID)

	_, ok = c.GetRegionDescriptorByKey([]byte{})
	require.True(t, ok)
}

func TestClusterRejectsStaleRegionHeartbeat(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 3})))

	err := c.PublishRegionDescriptor(testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 99}))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionHeartbeatStale)
}

func TestClusterRejectsOverlappingRegionRanges(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(1, []byte("a"), []byte("k"), metaregion.Epoch{Version: 1, ConfVersion: 1})))

	err := c.PublishRegionDescriptor(testDescriptor(2, []byte("j"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)
}

func TestClusterAllowsReplacingSameRegionWithNewEpoch(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(7, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})))

	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(7, []byte("a"), []byte("n"), metaregion.Epoch{Version: 2, ConfVersion: 1})))
	desc, ok := c.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(7), desc.RegionID)
	require.Equal(t, []byte("n"), desc.EndKey)
}

func TestClusterValidateRegionDescriptorDoesNotMutate(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})))

	err := c.ValidateRegionDescriptor(testDescriptor(2, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)

	desc, ok := c.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(1), desc.RegionID)
	_, ok = c.GetRegionDescriptorByKey([]byte("x"))
	require.False(t, ok)
}

func TestClusterValidateRootEventDoesNotMutate(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})))

	err := c.ValidateRootEvent(rootevent.RegionDescriptorPublished(
		testDescriptor(2, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}),
	))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)

	desc, ok := c.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(1), desc.RegionID)
	_, ok = c.GetRegionDescriptorByKey([]byte("x"))
	require.False(t, ok)
}

func TestClusterRemoveRegion(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(1, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})))

	_, ok := c.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)

	removed := c.RemoveRegion(1)
	require.True(t, removed)

	_, ok = c.GetRegionDescriptorByKey([]byte("m"))
	require.False(t, ok)

	removed = c.RemoveRegion(1)
	require.False(t, removed)
}

func TestClusterReplaceRegionSnapshot(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})))
	require.NoError(t, c.PublishRegionDescriptor(testDescriptor(2, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})))

	c.ReplaceRegionSnapshot(map[uint64]descriptor.Descriptor{
		3: testDescriptor(3, []byte(""), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}),
	})

	_, ok := c.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	desc, ok := c.GetRegionDescriptorByKey([]byte("x"))
	require.True(t, ok)
	require.Equal(t, uint64(3), desc.RegionID)
	_, ok = c.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)
	require.Len(t, c.RegionSnapshot(), 1)
}

func TestClusterPublishRootEventTracksTransitionSnapshot(t *testing.T) {
	c := NewCluster()
	current := testDescriptor(20, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
	require.NoError(t, c.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.EnsureHash()

	require.NoError(t, c.PublishRootEvent(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)))
	transitions := c.TransitionSnapshot()
	require.Contains(t, transitions.PendingPeerChanges, target.RegionID)
	operators := c.OperatorSnapshot()
	require.Len(t, operators.Entries, 1)
	require.Equal(t, rootstate.TransitionStatusPending, operators.Entries[0].Transition.Status)

	require.NoError(t, c.PublishRootEvent(rootevent.PeerAdded(target.RegionID, 2, 201, target)))
	transitions = c.TransitionSnapshot()
	require.NotContains(t, transitions.PendingPeerChanges, target.RegionID)

	assessment := c.ObserveRootEventLifecycle(rootevent.PeerAdded(target.RegionID, 2, 201, target))
	require.Equal(t, rootstate.TransitionStatusCompleted, assessment.Status)
	require.Equal(t, rootstate.RootEventLifecycleSkip, assessment.Decision)
}

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    epoch,
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
