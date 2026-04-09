package storage

import (
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRootStoreRefreshFromReplicatedFollower(t *testing.T) {
	rootStores, leaderID := openReplicatedRootStores(t)
	leader := rootStores[leaderID]
	followerRoot := rootStores[followerID(leaderID)]
	follower := followerRoot

	desc := testDescriptor(71, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	require.NoError(t, leader.AppendRootEvent(rootevent.RegionBootstrapped(desc)))

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
	require.NoError(t, leader.AppendRootEvent(rootevent.RegionBootstrapped(desc)))

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

func TestRootStoreWaitForTailTracksAllocatorFenceCheckpoint(t *testing.T) {
	rootStores, leaderID := openReplicatedRootStores(t)
	leader := rootStores[leaderID]
	follower := rootStores[followerID(leaderID)]
	subscription := follower.SubscribeTail(rootstorage.TailToken{})
	require.NotNil(t, subscription)

	require.NoError(t, leader.SaveAllocatorState(123, 456))
	require.Eventually(t, func() bool {
		advance, err := subscription.Wait(500 * time.Millisecond)
		if err != nil {
			return false
		}
		switch advance.CatchUpAction() {
		case rootstorage.TailCatchUpRefreshState, rootstorage.TailCatchUpInstallBootstrap, rootstorage.TailCatchUpAcknowledgeWindow:
			subscription.Acknowledge(advance)
		}
		snapshot, err := follower.Load()
		if err != nil {
			return false
		}
		return snapshot.Allocator.IDCurrent == 123 && snapshot.Allocator.TSCurrent == 456
	}, 6*time.Second, 50*time.Millisecond)
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
