package catalog

import (
	pdview "github.com/feichai0017/NoKV/coordinator/view"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterStoreHeartbeatAndSnapshot(t *testing.T) {
	c := NewCluster()
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

func TestClusterReplaceRootSnapshotPreservesStoreStateAndRefreshesRuntime(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.UpsertStoreHeartbeat(pdview.StoreStats{StoreID: 9, RegionNum: 2}))

	base := testDescriptor(30, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
	target := base.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.EnsureHash()

	c.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{base.RegionID: base},
		map[uint64]rootstate.PendingPeerChange{
			base.RegionID: {
				Kind:    rootstate.PendingPeerChangeAddition,
				StoreID: 2,
				PeerID:  201,
				Base:    base,
				Target:  target,
			},
		},
		nil,
	)

	require.Len(t, c.StoreSnapshot(), 1)
	require.Len(t, c.RegionSnapshot(), 1)
	transitions := c.TransitionSnapshot()
	require.Contains(t, transitions.PendingPeerChanges, base.RegionID)
	operators := c.OperatorSnapshot()
	require.Len(t, operators.Entries, 1)
	require.Equal(t, uint64(30), operators.Entries[0].Transition.Key)
	require.Equal(t, "coordinator", operators.Entries[0].Owner)

	entry := operators.Entries[0]
	entry.Owner = "mutated"
	operators.Entries[0] = entry
	change := transitions.PendingPeerChanges[base.RegionID]
	change.Target.StartKey = []byte("mutated")
	transitions.PendingPeerChanges[base.RegionID] = change

	freshOperators := c.OperatorSnapshot()
	freshTransitions := c.TransitionSnapshot()
	require.Equal(t, "coordinator", freshOperators.Entries[0].Owner)
	require.Equal(t, []byte("a"), freshTransitions.PendingPeerChanges[base.RegionID].Target.StartKey)

	c.ReplaceRootSnapshot(nil, nil, nil)
	require.Len(t, c.StoreSnapshot(), 1)
	require.Empty(t, c.RegionSnapshot())
	require.Empty(t, c.TransitionSnapshot().PendingPeerChanges)
	require.Empty(t, c.OperatorSnapshot().Entries)
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
