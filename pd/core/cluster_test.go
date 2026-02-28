package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
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
	require.NoError(t, c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       1,
		StartKey: []byte(""),
		EndKey:   []byte("m"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
	}))
	require.NoError(t, c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       2,
		StartKey: []byte("m"),
		EndKey:   []byte(""),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
	}))

	meta, ok := c.GetRegionByKey([]byte("a"))
	require.True(t, ok)
	require.Equal(t, uint64(1), meta.ID)

	meta, ok = c.GetRegionByKey([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(2), meta.ID)

	meta, ok = c.GetRegionByKey([]byte("z"))
	require.True(t, ok)
	require.Equal(t, uint64(2), meta.ID)

	_, ok = c.GetRegionByKey([]byte{})
	require.True(t, ok)
}

func TestClusterRejectsStaleRegionHeartbeat(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    manifest.RegionEpoch{Version: 2, ConfVersion: 3},
	}))

	err := c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 99},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionHeartbeatStale)
}

func TestClusterRejectsOverlappingRegionRanges(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("k"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
	}))

	err := c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       2,
		StartKey: []byte("j"),
		EndKey:   []byte("z"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)
}

func TestClusterAllowsReplacingSameRegionWithNewEpoch(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       7,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
	}))

	require.NoError(t, c.UpsertRegionHeartbeat(manifest.RegionMeta{
		ID:       7,
		StartKey: []byte("a"),
		EndKey:   []byte("n"),
		Epoch:    manifest.RegionEpoch{Version: 2, ConfVersion: 1},
	}))
	meta, ok := c.GetRegionByKey([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(7), meta.ID)
	require.Equal(t, []byte("n"), meta.EndKey)
}
