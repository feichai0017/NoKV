package view

import (
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
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
	require.NoError(t, v.UpsertAt(localmeta.RegionMeta{
		ID:       1,
		StartKey: []byte(""),
		EndKey:   []byte("m"),
		Epoch:    localmeta.RegionEpoch{Version: 1, ConfVersion: 1},
	}, now))
	require.NoError(t, v.UpsertAt(localmeta.RegionMeta{
		ID:       2,
		StartKey: []byte("m"),
		EndKey:   []byte(""),
		Epoch:    localmeta.RegionEpoch{Version: 1, ConfVersion: 1},
	}, now))

	meta, ok := v.Lookup([]byte("a"))
	require.True(t, ok)
	require.Equal(t, uint64(1), meta.ID)

	meta, ok = v.Lookup([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(2), meta.ID)

	ts, ok := v.LastHeartbeat(2)
	require.True(t, ok)
	require.Equal(t, now, ts)

	err := v.UpsertAt(localmeta.RegionMeta{
		ID:       2,
		StartKey: []byte("m"),
		EndKey:   []byte(""),
		Epoch:    localmeta.RegionEpoch{Version: 0, ConfVersion: 1},
	}, now)
	require.ErrorIs(t, err, ErrRegionHeartbeatStale)

	err = v.UpsertAt(localmeta.RegionMeta{
		ID:       3,
		StartKey: []byte("l"),
		EndKey:   []byte("z"),
		Epoch:    localmeta.RegionEpoch{Version: 1, ConfVersion: 1},
	}, now)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)
}
