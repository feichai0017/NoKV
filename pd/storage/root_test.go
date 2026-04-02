package storage

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/local"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootStorePersistsRegionsAndAllocator(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)

	store, err := OpenRootStore(root)
	require.NoError(t, err)

	meta := localmeta.RegionMeta{
		ID:       11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: localmeta.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []localmeta.PeerMeta{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: localmeta.RegionStateRunning,
	}
	require.NoError(t, store.PublishRegionDescriptor(meta))
	require.NoError(t, store.SaveAllocatorState(123, 456))

	snapshot, err := store.Load()
	require.NoError(t, err)
	got, ok := snapshot.Descriptors[meta.ID]
	require.True(t, ok)
	require.Equal(t, meta.ID, got.RegionID)
	require.Equal(t, meta.StartKey, got.StartKey)
	require.Equal(t, meta.EndKey, got.EndKey)
	require.Equal(t, meta.Peers, got.Peers)
	require.Equal(t, uint64(123), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(456), snapshot.Allocator.TSCurrent)

	reopened, err := OpenRootStore(root)
	require.NoError(t, err)
	loaded, err := reopened.Load()
	require.NoError(t, err)
	require.Contains(t, loaded.Descriptors, meta.ID)
	require.Equal(t, uint64(123), loaded.Allocator.IDCurrent)
	require.Equal(t, uint64(456), loaded.Allocator.TSCurrent)
}

func TestRootStoreDeleteRegion(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	require.NoError(t, store.PublishRegionDescriptor(localmeta.RegionMeta{
		ID:       7,
		StartKey: []byte("x"),
		EndKey:   []byte("z"),
		Epoch: localmeta.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
		State: localmeta.RegionStateRunning,
	}))
	require.NoError(t, store.TombstoneRegion(7))

	snapshot, err := store.Load()
	require.NoError(t, err)
	_, ok := snapshot.Descriptors[7]
	require.False(t, ok)

	reopened, err := OpenRootStore(root)
	require.NoError(t, err)
	loaded, err := reopened.Load()
	require.NoError(t, err)
	_, ok = loaded.Descriptors[7]
	require.False(t, ok)
}

func TestRootStoreSkipsDuplicateRegionDescriptorHeartbeat(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	meta := localmeta.RegionMeta{
		ID:       21,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    localmeta.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []localmeta.PeerMeta{
			{StoreID: 1, PeerID: 101},
		},
		State: localmeta.RegionStateRunning,
	}
	require.NoError(t, store.PublishRegionDescriptor(meta))
	require.NoError(t, store.PublishRegionDescriptor(meta))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, rootpkg.EventKindRegionBootstrap, events[0].Kind)
}

func TestRootStoreEmitsPeerAddedEvent(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	meta := localmeta.RegionMeta{
		ID:       31,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    localmeta.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []localmeta.PeerMeta{
			{StoreID: 1, PeerID: 101},
		},
		State: localmeta.RegionStateRunning,
	}
	require.NoError(t, store.PublishRegionDescriptor(meta))
	meta.Peers = append(meta.Peers, localmeta.PeerMeta{StoreID: 2, PeerID: 201})
	meta.Epoch.ConfVersion = 2
	require.NoError(t, store.PublishRegionDescriptor(meta))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootpkg.EventKindRegionBootstrap, events[0].Kind)
	require.Equal(t, rootpkg.EventKindPeerAdded, events[1].Kind)
	require.NotNil(t, events[1].PeerChange)
	require.Equal(t, uint64(2), events[1].PeerChange.StoreID)
	require.Equal(t, uint64(201), events[1].PeerChange.PeerID)
}

func TestRootStoreEmitsPeerRemovedEvent(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	meta := localmeta.RegionMeta{
		ID:       41,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    localmeta.RegionEpoch{Version: 1, ConfVersion: 2},
		Peers: []localmeta.PeerMeta{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: localmeta.RegionStateRunning,
	}
	require.NoError(t, store.PublishRegionDescriptor(meta))
	meta.Peers = meta.Peers[:1]
	meta.Epoch.ConfVersion = 3
	require.NoError(t, store.PublishRegionDescriptor(meta))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootpkg.EventKindPeerRemoved, events[1].Kind)
	require.NotNil(t, events[1].PeerChange)
	require.Equal(t, uint64(2), events[1].PeerChange.StoreID)
	require.Equal(t, uint64(201), events[1].PeerChange.PeerID)
}

func TestOpenRootLocalStoreCreatesMetadataRootFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenRootLocalStore(dir)
	require.NoError(t, err)

	require.NoError(t, store.SaveAllocatorState(9, 17))

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(9), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(17), snapshot.Allocator.TSCurrent)

	require.FileExists(t, filepath.Join(dir, rootlocal.CheckpointFileName))
}
