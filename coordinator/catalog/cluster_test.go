package catalog

import (
	pdview "github.com/feichai0017/NoKV/coordinator/view"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterStoreHeartbeatAndSnapshot(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRootEvent(rootevent.StoreJoined(1)))
	require.NoError(t, c.PublishRootEvent(rootevent.StoreJoined(2)))
	require.NoError(t, c.UpsertStoreHeartbeat(pdview.StoreStats{StoreID: 1, RegionNum: 3}))
	require.NoError(t, c.UpsertStoreHeartbeat(pdview.StoreStats{StoreID: 2, RegionNum: 5}))

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

func TestClusterStoreHeartbeatRequiresActiveMembership(t *testing.T) {
	c := NewCluster()
	err := c.UpsertStoreHeartbeat(pdview.StoreStats{StoreID: 7})
	require.ErrorIs(t, err, ErrStoreNotJoined)

	require.NoError(t, c.PublishRootEvent(rootevent.StoreJoined(7)))
	require.NoError(t, c.UpsertStoreHeartbeat(pdview.StoreStats{StoreID: 7}))
	info, ok := c.StoreInfoByID(7)
	require.True(t, ok)
	require.Equal(t, rootstate.StoreMembershipActive, info.Membership.State)
	require.True(t, info.HasRuntime)

	require.NoError(t, c.PublishRootEvent(rootevent.StoreRetired(7)))
	err = c.UpsertStoreHeartbeat(pdview.StoreStats{StoreID: 7})
	require.ErrorIs(t, err, ErrStoreRetired)
	info, ok = c.StoreInfoByID(7)
	require.True(t, ok)
	require.Equal(t, rootstate.StoreMembershipRetired, info.Membership.State)
	require.False(t, info.HasRuntime)
}

func TestClusterMountLifecycleRootEvents(t *testing.T) {
	c := NewCluster()

	require.NoError(t, c.PublishRootEvent(rootevent.MountRegistered("vol", 1, 1)))
	mount, ok := c.MountByID("vol")
	require.True(t, ok)
	require.Equal(t, rootstate.MountRecord{
		MountID:       "vol",
		RootInode:     1,
		SchemaVersion: 1,
		State:         rootstate.MountStateActive,
	}, mount)

	require.NoError(t, c.ValidateRootEvent(rootevent.MountRegistered("vol", 1, 1)))
	err := c.ValidateRootEvent(rootevent.MountRegistered("vol", 2, 1))
	require.ErrorIs(t, err, ErrMountConflict)

	require.NoError(t, c.PublishRootEvent(rootevent.MountRetired("vol")))
	mount, ok = c.MountByID("vol")
	require.True(t, ok)
	require.Equal(t, rootstate.MountStateRetired, mount.State)

	err = c.ValidateRootEvent(rootevent.MountRegistered("vol", 1, 1))
	require.ErrorIs(t, err, ErrMountRetired)
	err = c.ValidateRootEvent(rootevent.MountRetired("missing"))
	require.ErrorIs(t, err, ErrMountNotFound)
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

func TestClusterRegionAccessorsAndHeartbeat(t *testing.T) {
	c := NewCluster()
	desc := testDescriptor(4, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
	require.NoError(t, c.PublishRegionDescriptor(desc))

	require.True(t, c.HasRegion(4))
	require.False(t, c.HasRegion(99))

	got, ok := c.GetRegionDescriptor(4)
	require.True(t, ok)
	require.Equal(t, desc.RegionID, got.RegionID)
	got.StartKey = []byte("mutated")
	fresh, ok := c.GetRegionDescriptor(4)
	require.True(t, ok)
	require.Equal(t, []byte("a"), fresh.StartKey)

	require.True(t, c.TouchRegionHeartbeat(4))
	lastHB, ok := c.RegionLastHeartbeat(4)
	require.True(t, ok)
	require.False(t, lastHB.IsZero())
	require.False(t, c.TouchRegionHeartbeat(404))
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
	entries := rootstate.BuildTransitionEntries(rootstate.Snapshot{
		Descriptors:         map[uint64]descriptor.Descriptor{target.RegionID: current},
		PendingPeerChanges:  transitions.PendingPeerChanges,
		PendingRangeChanges: transitions.PendingRangeChanges,
	})
	require.Len(t, entries, 1)
	require.Equal(t, rootstate.TransitionStatusPending, entries[0].Status)

	require.NoError(t, c.PublishRootEvent(rootevent.PeerAdded(target.RegionID, 2, 201, target)))
	transitions = c.TransitionSnapshot()
	require.NotContains(t, transitions.PendingPeerChanges, target.RegionID)

	assessment := c.ObserveRootEventLifecycle(rootevent.PeerAdded(target.RegionID, 2, 201, target))
	require.Equal(t, rootstate.TransitionStatusCompleted, assessment.Status)
	require.Equal(t, rootstate.RootEventLifecycleSkip, assessment.Decision)
}

func TestClusterReplaceRootSnapshotPreservesStoreStateAndRefreshesRuntime(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRootEvent(rootevent.StoreJoined(9)))
	require.NoError(t, c.UpsertStoreHeartbeat(pdview.StoreStats{StoreID: 9, RegionNum: 2}))

	base := testDescriptor(30, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
	target := base.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.EnsureHash()

	c.ReplaceRootSnapshot(rootstate.Snapshot{
		Stores:      map[uint64]rootstate.StoreMembership{9: {StoreID: 9, State: rootstate.StoreMembershipActive}},
		Descriptors: map[uint64]descriptor.Descriptor{base.RegionID: base},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
			base.RegionID: {
				Kind:    rootstate.PendingPeerChangeAddition,
				StoreID: 2,
				PeerID:  201,
				Base:    base,
				Target:  target,
			},
		},
	}, rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 9}, Revision: 4})

	require.Len(t, c.StoreSnapshot(), 1)
	require.Len(t, c.RegionSnapshot(), 1)
	transitions := c.TransitionSnapshot()
	require.Contains(t, transitions.PendingPeerChanges, base.RegionID)
	change := transitions.PendingPeerChanges[base.RegionID]
	change.Target.StartKey = []byte("mutated")
	transitions.PendingPeerChanges[base.RegionID] = change

	freshTransitions := c.TransitionSnapshot()
	require.Equal(t, []byte("a"), freshTransitions.PendingPeerChanges[base.RegionID].Target.StartKey)
	require.Equal(t, uint64(4), c.CatalogRootToken().Revision)

	c.ReplaceRootSnapshot(rootstate.Snapshot{Stores: map[uint64]rootstate.StoreMembership{9: {StoreID: 9, State: rootstate.StoreMembershipActive}}}, rootstorage.TailToken{})
	require.Len(t, c.StoreSnapshot(), 1)
	require.Empty(t, c.RegionSnapshot())
	require.Empty(t, c.TransitionSnapshot().PendingPeerChanges)
}

func TestClusterPublishRootEventCoversTopologyLifecycleBranches(t *testing.T) {
	t.Run("region removal", func(t *testing.T) {
		c := NewCluster()
		desc := testDescriptor(50, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		require.NoError(t, c.PublishRegionDescriptor(desc))
		require.NoError(t, c.PublishRootEvent(rootevent.RegionTombstoned(desc.RegionID)))
		require.False(t, c.HasRegion(desc.RegionID))
	})

	t.Run("peer addition cancelled without base removes region", func(t *testing.T) {
		c := NewCluster()
		desc := testDescriptor(51, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		require.NoError(t, c.PublishRegionDescriptor(desc))
		require.NoError(t, c.PublishRootEvent(rootevent.PeerAdditionCancelled(desc.RegionID, 2, 201, desc, descriptor.Descriptor{})))
		require.False(t, c.HasRegion(desc.RegionID))
	})

	t.Run("peer removal cancelled restores base descriptor", func(t *testing.T) {
		c := NewCluster()
		base := testDescriptor(52, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		current := base.Clone()
		current.EndKey = []byte("y")
		current.EnsureHash()
		require.NoError(t, c.PublishRegionDescriptor(current))
		require.NoError(t, c.PublishRootEvent(rootevent.PeerRemovalCancelled(base.RegionID, 2, 201, current, base)))
		got, ok := c.GetRegionDescriptor(base.RegionID)
		require.True(t, ok)
		require.Equal(t, base.EndKey, got.EndKey)
		require.Equal(t, base.Epoch, got.Epoch)
	})

	t.Run("split committed replaces parent with children", func(t *testing.T) {
		c := NewCluster()
		parent := testDescriptor(53, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		left := testDescriptor(53, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1})
		right := testDescriptor(54, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		require.NoError(t, c.PublishRegionDescriptor(parent))
		require.NoError(t, c.PublishRootEvent(rootevent.RegionSplitCommitted(parent.RegionID, []byte("m"), left, right)))
		gotLeft, ok := c.GetRegionDescriptor(left.RegionID)
		require.True(t, ok)
		require.Equal(t, left.EndKey, gotLeft.EndKey)
		gotRight, ok := c.GetRegionDescriptor(right.RegionID)
		require.True(t, ok)
		require.Equal(t, right.StartKey, gotRight.StartKey)
	})

	t.Run("split cancelled restores base parent", func(t *testing.T) {
		c := NewCluster()
		base := testDescriptor(55, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		left := testDescriptor(55, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1})
		right := testDescriptor(56, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		require.NoError(t, c.PublishRegionDescriptor(left))
		require.NoError(t, c.PublishRegionDescriptor(right))
		require.NoError(t, c.PublishRootEvent(rootevent.RegionSplitCancelled(base.RegionID, []byte("m"), left, right, base)))
		got, ok := c.GetRegionDescriptor(base.RegionID)
		require.True(t, ok)
		require.Equal(t, base.StartKey, got.StartKey)
		require.Equal(t, base.EndKey, got.EndKey)
		require.False(t, c.HasRegion(right.RegionID))
	})

	t.Run("merge committed replaces left and right with merged", func(t *testing.T) {
		c := NewCluster()
		left := testDescriptor(57, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		right := testDescriptor(58, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		merged := testDescriptor(57, []byte("a"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1})
		require.NoError(t, c.PublishRegionDescriptor(left))
		require.NoError(t, c.PublishRegionDescriptor(right))
		require.NoError(t, c.PublishRootEvent(rootevent.RegionMerged(left.RegionID, right.RegionID, merged)))
		require.False(t, c.HasRegion(right.RegionID))
		got, ok := c.GetRegionDescriptor(merged.RegionID)
		require.True(t, ok)
		require.Equal(t, merged.EndKey, got.EndKey)
	})

	t.Run("merge cancelled restores left and right", func(t *testing.T) {
		c := NewCluster()
		baseLeft := testDescriptor(59, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		baseRight := testDescriptor(60, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
		merged := testDescriptor(59, []byte("a"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1})
		require.NoError(t, c.PublishRegionDescriptor(merged))
		require.NoError(t, c.PublishRootEvent(rootevent.RegionMergeCancelled(baseLeft.RegionID, baseRight.RegionID, merged, baseLeft, baseRight)))
		gotLeft, ok := c.GetRegionDescriptor(baseLeft.RegionID)
		require.True(t, ok)
		require.Equal(t, baseLeft.EndKey, gotLeft.EndKey)
		gotRight, ok := c.GetRegionDescriptor(baseRight.RegionID)
		require.True(t, ok)
		require.Equal(t, baseRight.StartKey, gotRight.StartKey)
	})
}

func TestClusterLeaderClaimsAndPendingRangeHelpers(t *testing.T) {
	c := NewCluster()
	left := testDescriptor(70, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})
	right := testDescriptor(71, []byte("m"), []byte(""), metaregion.Epoch{Version: 2, ConfVersion: 1})
	require.NoError(t, c.PublishRegionDescriptor(left))
	require.NoError(t, c.PublishRegionDescriptor(right))

	c.RecordRegionLeaders(9, []uint64{left.RegionID, right.RegionID})
	snap := c.RegionSnapshot()
	require.Len(t, snap, 2)
	require.Equal(t, uint64(9), snap[0].LeaderStoreID)
	require.Equal(t, uint64(9), snap[1].LeaderStoreID)

	c.RecordRegionLeaders(9, []uint64{right.RegionID})
	snap = c.RegionSnapshot()
	require.Zero(t, snap[0].LeaderStoreID)
	require.Equal(t, uint64(9), snap[1].LeaderStoreID)

	require.Equal(t, right.RootEpoch, c.MaxDescriptorRevision())

	merged := testDescriptor(72, []byte(""), []byte(""), metaregion.Epoch{Version: 3, ConfVersion: 1})
	c.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]descriptor.Descriptor{merged.RegionID: merged},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
			merged.RegionID: {
				Kind:          rootstate.PendingRangeChangeMerge,
				LeftRegionID:  left.RegionID,
				RightRegionID: right.RegionID,
				Merged:        merged,
			},
		},
	}, rootstorage.TailToken{})

	change, ok := c.PendingRangeChangeForDescriptor(merged.RegionID)
	require.True(t, ok)
	require.Equal(t, rootstate.PendingRangeChangeMerge, change.Kind)
	_, ok = c.PendingRangeChangeForDescriptor(999)
	require.False(t, ok)
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
