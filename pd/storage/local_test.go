package storage

import (
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/stretchr/testify/require"
)

func TestLocalStoreLoadEmptySnapshot(t *testing.T) {
	store, err := OpenLocalStore(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Empty(t, snapshot.Regions)
	require.Equal(t, uint64(0), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(0), snapshot.Allocator.TSCurrent)
}

func TestLocalStorePersistsRegionsAndAllocator(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	meta := manifest.RegionMeta{
		ID:       11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
	}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.SaveAllocatorState(123, 456))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	snapshot, err := reopened.Load()
	require.NoError(t, err)
	got, ok := snapshot.Regions[meta.ID]
	require.True(t, ok)
	require.Equal(t, meta.ID, got.ID)
	require.Equal(t, meta.StartKey, got.StartKey)
	require.Equal(t, meta.EndKey, got.EndKey)
	require.Equal(t, uint64(123), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(456), snapshot.Allocator.TSCurrent)

	require.FileExists(t, filepath.Join(dir, StateFileName))
}

func TestLocalStoreDeleteRegion(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	meta := manifest.RegionMeta{
		ID:       7,
		StartKey: []byte("x"),
		EndKey:   []byte("z"),
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
	}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.DeleteRegion(meta.ID))

	snapshot, err := store.Load()
	require.NoError(t, err)
	_, ok := snapshot.Regions[meta.ID]
	require.False(t, ok)
}
