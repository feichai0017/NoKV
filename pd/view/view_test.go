package view

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreHealthViewSnapshot(t *testing.T) {
	v := NewStoreHealthView()
	ts := time.Unix(100, 0)
	require.NoError(t, v.UpsertAt(StoreStats{StoreID: 2, RegionNum: 5}, ts))
	require.NoError(t, v.UpsertAt(StoreStats{StoreID: 1, RegionNum: 3}, ts))

	snap := v.Snapshot()
	require.Len(t, snap, 2)
	require.Equal(t, uint64(1), snap[0].StoreID)
	require.Equal(t, ts, snap[0].UpdatedAt)

	v.Remove(1)
	snap = v.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(2), snap[0].StoreID)
}

func TestRegionDirectoryViewLookupAndValidation(t *testing.T) {
	v := NewRegionDirectoryView()
	now := time.Unix(200, 0)
	require.NoError(t, v.UpsertAt(testDescriptor(1, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}), now))
	require.NoError(t, v.UpsertAt(testDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 1, ConfVersion: 1}), now))

	got, ok := v.LookupDescriptor([]byte("a"))
	require.True(t, ok)
	require.Equal(t, uint64(1), got.RegionID)

	got, ok = v.LookupDescriptor([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(2), got.RegionID)

	ts, ok := v.LastHeartbeat(2)
	require.True(t, ok)
	require.Equal(t, now, ts)

	err := v.UpsertAt(testDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 0, ConfVersion: 1}), now)
	require.ErrorIs(t, err, ErrRegionHeartbeatStale)

	err = v.UpsertAt(testDescriptor(3, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}), now)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)
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
