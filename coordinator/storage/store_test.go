package storage

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/stretchr/testify/require"
)

func TestNoopStoreLoadInitializesRegionsMap(t *testing.T) {
	store := noopStore{}
	snapshot, err := store.Load()
	require.NoError(t, err)
	require.NotNil(t, snapshot.Descriptors)
	require.Empty(t, snapshot.Descriptors)
}

func TestNoopStoreMethodsAreStableNoOps(t *testing.T) {
	store := noopStore{}

	require.NoError(t, store.AppendRootEvent(rootevent.Event{}))
	require.NoError(t, store.SaveAllocatorState(1, 2))
	require.NoError(t, store.Refresh())
	require.True(t, store.IsLeader())
	require.Equal(t, uint64(0), store.LeaderID())
	require.NoError(t, store.Close())
}
