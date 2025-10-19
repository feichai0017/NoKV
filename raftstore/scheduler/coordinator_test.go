package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
)

func TestCoordinatorTracksRegionHeartbeats(t *testing.T) {
	c := NewCoordinator()
	region := manifest.RegionMeta{ID: 1, StartKey: []byte("a"), EndKey: []byte("b"), Peers: []manifest.PeerMeta{{StoreID: 1, PeerID: 11}}}

	c.SubmitRegionHeartbeat(region)

	snapshot := c.RegionSnapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, uint64(1), snapshot[0].ID)

	prev, ok := c.LastUpdate(1)
	require.True(t, ok)
	require.False(t, prev.IsZero())

	region.Peers = append(region.Peers, manifest.PeerMeta{StoreID: 2, PeerID: 22})
	c.SubmitRegionHeartbeat(region)

	snapshot = c.RegionSnapshot()
	require.Len(t, snapshot, 1)
	require.Len(t, snapshot[0].Peers, 2)

	c.RemoveRegion(1)
	snapshot = c.RegionSnapshot()
	require.Empty(t, snapshot)

	_, ok = c.LastUpdate(1)
	require.False(t, ok)
}

func TestCoordinatorIgnoresInvalidInput(t *testing.T) {
	c := NewCoordinator()
	c.SubmitRegionHeartbeat(manifest.RegionMeta{})
	require.Empty(t, c.RegionSnapshot())

	c.RemoveRegion(0)
	require.Empty(t, c.RegionSnapshot())

	_, ok := c.LastUpdate(0)
	require.False(t, ok)

	// Ensure LastUpdate is monotonic.
	region := manifest.RegionMeta{ID: 2}
	c.SubmitRegionHeartbeat(region)
	first, ok := c.LastUpdate(2)
	require.True(t, ok)

	time.Sleep(10 * time.Millisecond)
	c.SubmitRegionHeartbeat(region)
	second, ok := c.LastUpdate(2)
	require.True(t, ok)
	require.True(t, second.After(first) || second.Equal(first))
}

func TestCoordinatorTracksStoreStats(t *testing.T) {
	c := NewCoordinator()
	c.SubmitStoreHeartbeat(StoreStats{StoreID: 5, Capacity: 1 << 40, Available: 1 << 39, RegionNum: 12})

	stats := c.StoreSnapshot()
	require.Len(t, stats, 1)
	require.Equal(t, uint64(5), stats[0].StoreID)
	require.True(t, stats[0].UpdatedAt.After(time.Time{}))

	c.SubmitStoreHeartbeat(StoreStats{StoreID: 5, Capacity: 1 << 41, Available: 1 << 40, RegionNum: 20})
	stats = c.StoreSnapshot()
	require.Len(t, stats, 1)
	require.Equal(t, uint64(20), stats[0].RegionNum)

	c.RemoveStore(5)
	stats = c.StoreSnapshot()
	require.Empty(t, stats)
}
