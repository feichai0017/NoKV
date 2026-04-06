package view

import (
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

func TestStoreHealthViewRejectsInvalidStoreID(t *testing.T) {
	v := NewStoreHealthView()
	err := v.UpsertAt(StoreStats{StoreID: 0}, time.Unix(200, 0))
	require.ErrorIs(t, err, ErrInvalidStoreID)
	require.Empty(t, v.Snapshot())
}
