package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoopStoreLoadInitializesRegionsMap(t *testing.T) {
	store := NewNoopStore()
	snapshot, err := store.Load()
	require.NoError(t, err)
	require.NotNil(t, snapshot.Regions)
	require.Empty(t, snapshot.Regions)
}

func TestResolveAllocatorStartsBasic(t *testing.T) {
	id, ts := ResolveAllocatorStarts(1, 100, AllocatorState{
		IDCurrent: 50,
		TSCurrent: 20,
	})
	require.Equal(t, uint64(51), id)
	require.Equal(t, uint64(100), ts)

	id, ts = ResolveAllocatorStarts(80, 30, AllocatorState{
		IDCurrent: 50,
		TSCurrent: 20,
	})
	require.Equal(t, uint64(80), id)
	require.Equal(t, uint64(30), ts)
}

func TestResolveAllocatorStartsHandlesUint64Overflow(t *testing.T) {
	id, ts := ResolveAllocatorStarts(1, 1, AllocatorState{
		IDCurrent: math.MaxUint64,
		TSCurrent: math.MaxUint64,
	})
	require.Equal(t, uint64(math.MaxUint64), id)
	require.Equal(t, uint64(math.MaxUint64), ts)
}
