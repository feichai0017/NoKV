package storage

import (
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"net"
	"path/filepath"
	"testing"
	"time"

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

	events, _, err := root.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, rootevent.KindRegionBootstrap, events[0].Kind)
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
	require.NoError(t, store.AppendRootEvent(rootevent.PeerAdded(desc.RegionID, 2, 201, desc)))

	events, _, err := root.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootevent.KindRegionBootstrap, events[0].Kind)
	require.Equal(t, rootevent.KindPeerAdded, events[1].Kind)
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
	require.NoError(t, store.AppendRootEvent(rootevent.PeerRemoved(desc.RegionID, 2, 201, desc)))

	events, _, err := root.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootevent.KindPeerRemoved, events[1].Kind)
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
	require.NoError(t, store.AppendRootEvent(rootevent.RegionSplitCommitted(51, []byte("m"), parent, childDesc)))

	events, _, err := root.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, rootevent.KindRegionSplitCommitted, events[1].Kind)
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
	require.NoError(t, store.AppendRootEvent(rootevent.RegionMerged(61, 62, mergedDesc)))

	events, _, err := root.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, rootevent.KindRegionMerged, events[2].Kind)
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

	require.FileExists(t, filepath.Join(dir, rootfile.CheckpointFileName))
}

func TestRootStoreRefreshFromReplicatedFollower(t *testing.T) {
	rootStores, leaderID := openReplicatedRootStores(t)
	leader := rootStores[leaderID]
	followerRoot := rootStores[followerID(leaderID)]
	follower := followerRoot

	desc := testDescriptor(71, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	require.NoError(t, leader.PublishRegionDescriptor(desc))

	snapshot, err := follower.Load()
	require.NoError(t, err)
	_, ok := snapshot.Descriptors[71]
	require.False(t, ok)

	require.Eventually(t, func() bool {
		if err := followerRoot.Refresh(); err != nil {
			return false
		}
		if err := follower.Refresh(); err != nil {
			return false
		}
		snapshot, err = follower.Load()
		if err != nil {
			return false
		}
		got, ok := snapshot.Descriptors[71]
		return ok && got.RegionID == 71
	}, 5*time.Second, 50*time.Millisecond)
}

func TestOpenRootReplicatedStoreSharesThreeNodeCluster(t *testing.T) {
	rootStores, leaderID := openReplicatedRootStores(t)
	leader := rootStores[leaderID]
	follower := rootStores[followerID(leaderID)]

	desc := testDescriptor(81, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	require.NoError(t, leader.PublishRegionDescriptor(desc))

	require.Eventually(t, func() bool {
		if err := follower.Refresh(); err != nil {
			return false
		}
		snapshot, err := follower.Load()
		if err != nil {
			return false
		}
		got, ok := snapshot.Descriptors[81]
		return ok && got.RegionID == 81
	}, 5*time.Second, 50*time.Millisecond)
}

func TestReplicatedRootConfigValidate(t *testing.T) {
	cfg := ReplicatedRootConfig{
		WorkDir:       t.TempDir(),
		NodeID:        1,
		TransportAddr: "127.0.0.1:7001",
		PeerAddrs: map[uint64]string{
			1: "127.0.0.1:7001",
			2: "127.0.0.1:7002",
			3: "127.0.0.1:7003",
		},
	}
	require.NoError(t, cfg.Validate())

	cfg.PeerAddrs = map[uint64]string{1: "127.0.0.1:7001"}
	require.Error(t, cfg.Validate())
	cfg.PeerAddrs = map[uint64]string{
		2: "127.0.0.1:7002",
		3: "127.0.0.1:7003",
		4: "127.0.0.1:7004",
	}
	require.Error(t, cfg.Validate())
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

func openReplicatedRootStores(t *testing.T) (map[uint64]*RootStore, uint64) {
	t.Helper()

	peerAddrs := reserveRootPeerAddrs(t)
	rootStores := make(map[uint64]*RootStore, 3)
	for _, id := range []uint64{1, 2, 3} {
		store, err := OpenRootReplicatedStore(ReplicatedRootConfig{
			WorkDir:       filepath.Join(t.TempDir(), fmt.Sprintf("root-%d", id)),
			NodeID:        id,
			TransportAddr: peerAddrs[id],
			PeerAddrs:     peerAddrs,
		})
		require.NoError(t, err)
		rootStores[id] = store
		t.Cleanup(func() { require.NoError(t, store.Close()) })
	}

	var leaderID uint64
	require.Eventually(t, func() bool {
		for id, store := range rootStores {
			if store.IsLeader() {
				leaderID = id
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)
	return rootStores, leaderID
}

func reserveRootPeerAddrs(t *testing.T) map[uint64]string {
	t.Helper()
	out := make(map[uint64]string, 3)
	for _, id := range []uint64{1, 2, 3} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		out[id] = ln.Addr().String()
		require.NoError(t, ln.Close())
	}
	return out
}

func followerID(leaderID uint64) uint64 {
	for _, id := range []uint64{1, 2, 3} {
		if id != leaderID {
			return id
		}
	}
	return 0
}
