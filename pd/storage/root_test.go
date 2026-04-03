package storage

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootStorePersistsRegionsAndAllocator(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)

	store, err := OpenRootStore(root)
	require.NoError(t, err)

	desc := testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})
	require.NoError(t, store.PublishRegionDescriptor(desc))
	require.NoError(t, store.SaveAllocatorState(123, 456))

	snapshot, err := store.Load()
	require.NoError(t, err)
	got, ok := snapshot.Descriptors[desc.RegionID]
	require.True(t, ok)
	require.Equal(t, desc.RegionID, got.RegionID)
	require.Equal(t, desc.StartKey, got.StartKey)
	require.Equal(t, desc.EndKey, got.EndKey)
	require.Equal(t, desc.Peers, got.Peers)
	require.Equal(t, uint64(123), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(456), snapshot.Allocator.TSCurrent)

	reopened, err := OpenRootStore(root)
	require.NoError(t, err)
	loaded, err := reopened.Load()
	require.NoError(t, err)
	require.Contains(t, loaded.Descriptors, desc.RegionID)
	require.Equal(t, uint64(123), loaded.Allocator.IDCurrent)
	require.Equal(t, uint64(456), loaded.Allocator.TSCurrent)
}

func TestRootStoreDeleteRegion(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	require.NoError(t, store.PublishRegionDescriptor(testDescriptor(7, []byte("x"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)))
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

	desc := testDescriptor(21, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	require.NoError(t, store.PublishRegionDescriptor(desc))
	require.NoError(t, store.PublishRegionDescriptor(desc))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, rootpkg.EventKindRegionBootstrap, events[0].Kind)
}

func TestRootStoreAppendRootEventPeerAdded(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	desc := testDescriptor(31, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	require.NoError(t, store.PublishRegionDescriptor(desc))
	desc.Peers = append(desc.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	desc.Epoch.ConfVersion = 2
	desc.Hash = nil
	desc.EnsureHash()
	require.NoError(t, store.AppendRootEvent(rootpkg.PeerAdded(desc.RegionID, 2, 201, desc)))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootpkg.EventKindRegionBootstrap, events[0].Kind)
	require.Equal(t, rootpkg.EventKindPeerAdded, events[1].Kind)
	require.NotNil(t, events[1].PeerChange)
	require.Equal(t, uint64(2), events[1].PeerChange.StoreID)
	require.Equal(t, uint64(201), events[1].PeerChange.PeerID)
}

func TestRootStoreAppendRootEventPeerRemoved(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	desc := testDescriptor(41, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})
	require.NoError(t, store.PublishRegionDescriptor(desc))
	desc.Peers = desc.Peers[:1]
	desc.Epoch.ConfVersion = 3
	desc.Hash = nil
	desc.EnsureHash()
	require.NoError(t, store.AppendRootEvent(rootpkg.PeerRemoved(desc.RegionID, 2, 201, desc)))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootpkg.EventKindPeerRemoved, events[1].Kind)
	require.NotNil(t, events[1].PeerChange)
	require.Equal(t, uint64(2), events[1].PeerChange.StoreID)
	require.Equal(t, uint64(201), events[1].PeerChange.PeerID)
}

func TestRootStoreAppendRootEventSplitCommitted(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	parent := testDescriptor(51, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	require.NoError(t, store.PublishRegionDescriptor(parent))

	childDesc := testDescriptor(52, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 102}})
	parent.EndKey = []byte("m")
	parent.Epoch.Version = 2
	parent.Hash = nil
	parent.EnsureHash()
	require.NoError(t, store.AppendRootEvent(rootpkg.RegionSplitCommitted(51, []byte("m"), parent, childDesc)))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootpkg.EventKindRegionSplitCommitted, events[1].Kind)
	require.NotNil(t, events[1].RangeSplit)
	require.Equal(t, uint64(51), events[1].RangeSplit.ParentRegionID)
	require.Equal(t, uint64(51), events[1].RangeSplit.Left.RegionID)
	require.Equal(t, uint64(52), events[1].RangeSplit.Right.RegionID)
}

func TestRootStoreAppendRootEventRegionMerged(t *testing.T) {
	root, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	store, err := OpenRootStore(root)
	require.NoError(t, err)

	left := testDescriptor(61, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	right := testDescriptor(62, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 102}})
	require.NoError(t, store.PublishRegionDescriptor(left))
	require.NoError(t, store.PublishRegionDescriptor(right))

	left.EndKey = []byte("z")
	left.Epoch.Version = 2
	left.Hash = nil
	left.EnsureHash()
	mergedDesc := left
	require.NoError(t, store.AppendRootEvent(rootpkg.RegionMerged(61, 62, mergedDesc)))

	events, _, err := root.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, rootpkg.EventKindRegionMerged, events[2].Kind)
	require.NotNil(t, events[2].RangeMerge)
	require.Equal(t, uint64(61), events[2].RangeMerge.LeftRegionID)
	require.Equal(t, uint64(62), events[2].RangeMerge.RightRegionID)
	require.Equal(t, uint64(61), events[2].RangeMerge.Merged.RegionID)
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

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch, peers []metaregion.Peer) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    epoch,
		Peers:    append([]metaregion.Peer(nil), peers...),
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
