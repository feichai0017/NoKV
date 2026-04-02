package core

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
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
	require.NoError(t, c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       1,
		StartKey: []byte(""),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
	}, 0)))
	require.NoError(t, c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       2,
		StartKey: []byte("m"),
		EndKey:   []byte(""),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
	}, 0)))

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
	require.NoError(t, c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 3},
	}, 0)))

	err := c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 99},
	}, 0))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionHeartbeatStale)
}

func TestClusterRejectsOverlappingRegionRanges(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("k"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
	}, 0)))

	err := c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       2,
		StartKey: []byte("j"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
	}, 0))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)
}

func TestClusterAllowsReplacingSameRegionWithNewEpoch(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       7,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
	}, 0)))

	require.NoError(t, c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       7,
		StartKey: []byte("a"),
		EndKey:   []byte("n"),
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 1},
	}, 0)))
	desc, ok := c.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(7), desc.RegionID)
	require.Equal(t, []byte("n"), desc.EndKey)
}

func TestClusterRemoveRegion(t *testing.T) {
	c := NewCluster()
	require.NoError(t, c.PublishRegionDescriptor(descriptor.FromRegionMeta(localmeta.RegionMeta{
		ID:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
	}, 0)))

	_, ok := c.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)

	removed := c.RemoveRegion(1)
	require.True(t, removed)

	_, ok = c.GetRegionDescriptorByKey([]byte("m"))
	require.False(t, ok)

	removed = c.RemoveRegion(1)
	require.False(t, removed)
}
