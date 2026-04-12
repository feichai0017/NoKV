package controlplane

import (
	"context"
	"testing"

	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRootFenceAllocatorReserveAdvancesWindow(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)

	store, err := coordstorage.OpenRootStore(backend)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	a := &rootFenceAllocator{
		store:      store,
		current:    1,
		windowHigh: 0,
		tsFence:    0,
		windowSize: 4,
	}

	first, err := a.Reserve(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshot.Allocator.IDCurrent)

	first, err = a.Reserve(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), first)

	snapshot, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshot.Allocator.IDCurrent)

	first, err = a.Reserve(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), first)

	snapshot, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(6), snapshot.Allocator.IDCurrent)
}

func TestRootFenceAllocatorReserveUsesBatchWhenBatchExceedsWindow(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)

	store, err := coordstorage.OpenRootStore(backend)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	a := &rootFenceAllocator{
		store:      store,
		current:    1,
		windowHigh: 0,
		tsFence:    0,
		windowSize: 2,
	}

	first, err := a.Reserve(context.Background(), 5)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(5), snapshot.Allocator.IDCurrent)
}

func TestEtcdCurrentFenceParsesValue(t *testing.T) {
	current, modRev := etcdCurrentFence(&clientv3.GetResponse{})
	require.Equal(t, uint64(0), current)
	require.Equal(t, int64(0), modRev)
}
