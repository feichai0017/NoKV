package localmeta

import (
	"path/filepath"
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestLocalStorePersistsRegions(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	meta := RegionMeta{
		ID:       11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
	}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	got, ok := snapshot[meta.ID]
	require.True(t, ok)
	require.Equal(t, meta.ID, got.ID)
	require.Equal(t, meta.StartKey, got.StartKey)
	require.Equal(t, meta.EndKey, got.EndKey)
	require.FileExists(t, filepath.Join(dir, ReplicaStateFileName))
}

func TestLocalStoreDeleteRegion(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	meta := RegionMeta{ID: 7}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.DeleteRegion(meta.ID))
	_, ok := store.Snapshot()[meta.ID]
	require.False(t, ok)
}

func TestLocalStorePersistsRaftPointers(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	ptr := RaftLogPointer{
		GroupID:        7,
		Segment:        3,
		Offset:         2048,
		AppliedIndex:   42,
		AppliedTerm:    5,
		Committed:      41,
		SnapshotIndex:  64,
		SnapshotTerm:   7,
		TruncatedIndex: 11,
		TruncatedTerm:  2,
	}
	require.NoError(t, store.SaveRaftPointer(ptr))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	got, ok := reopened.RaftPointer(ptr.GroupID)
	require.True(t, ok)
	require.Equal(t, ptr, got)
	require.FileExists(t, filepath.Join(dir, RaftProgressFileName))
}

func TestLocalStorePersistsPendingRootEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	event := rootevent.PeerAdded(11, 2, 22, descriptor.Descriptor{
		RegionID: 11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
	})
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 7,
		Event:    event,
	}))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	pending := reopened.PendingRootEvents()
	require.Len(t, pending, 1)
	require.Equal(t, uint64(7), pending[0].Sequence)
	require.Equal(t, rootstate.TransitionIDFromEvent(event), rootstate.TransitionIDFromEvent(pending[0].Event))
	require.FileExists(t, filepath.Join(dir, PendingRootEventsFileName))
}

func TestLocalStoreDeletesPendingRootEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	event := rootevent.RegionTombstoned(9)
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 3,
		Event:    event,
	}))
	require.NoError(t, store.DeletePendingRootEvent(3))
	require.Empty(t, store.PendingRootEvents())
}
