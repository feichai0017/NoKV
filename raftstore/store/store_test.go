package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/store"
)

type noopTransport struct{}

func (noopTransport) Send(myraft.Message) {}

func TestStorePeerLifecycle(t *testing.T) {
	router := store.NewRouter()
	rs := store.NewStore(router)

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       100,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	require.NotNil(t, peer)

	metas := rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(100), metas[0].ID)

	require.NoError(t, router.SendTick(peer.ID()))
	require.NoError(t, router.BroadcastTick())
	require.NoError(t, router.BroadcastFlush())

	rs.StopPeer(peer.ID())
	_, ok := router.Peer(peer.ID())
	require.False(t, ok)
}

func TestStoreDuplicatePeer(t *testing.T) {
	rs := store.NewStore(nil)
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)

	defer rs.StopPeer(peer.ID())

	_, err = rs.StartPeer(cfg, nil)
	require.Error(t, err)
}

func TestStoreCustomFactoryAndHooks(t *testing.T) {
	router := store.NewRouter()
	factoryCalls := 0
	startCalls := 0
	stopCalls := 0

	rs := store.NewStoreWithConfig(store.Config{
		Router: router,
		PeerFactory: func(cfg *peer.Config) (*peer.Peer, error) {
			factoryCalls++
			return peer.NewPeer(cfg)
		},
		Hooks: store.LifecycleHooks{
			OnPeerStart: func(*peer.Peer) { startCalls++ },
			OnPeerStop:  func(*peer.Peer) { stopCalls++ },
		},
	})

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              2,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       200,
			StartKey: []byte("c"),
			EndKey:   []byte("d"),
		},
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, 1, startCalls)

	handles := rs.Peers()
	require.Len(t, handles, 1)
	require.NotNil(t, handles[0].Region)
	handles[0].Region.StartKey[0] = 'x'
	meta := peer.RegionMeta()
	require.Equal(t, byte('c'), meta.StartKey[0])

	rs.StopPeer(peer.ID())
	require.Equal(t, 1, stopCalls)
	require.Empty(t, rs.Peers())
}

func TestStorePersistsRegionMetadata(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	updateCalls := 0
	removeCalls := 0
	var states []manifest.RegionState

	rs := store.NewStoreWithConfig(store.Config{
		Manifest: mgr,
		RegionHooks: store.RegionHooks{
			OnRegionUpdate: func(meta manifest.RegionMeta) {
				updateCalls++
				states = append(states, meta.State)
			},
			OnRegionRemove: func(id uint64) {
				removeCalls++
			},
		},
	})

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              3,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       500,
			StartKey: []byte("k"),
			EndKey:   []byte("z"),
		},
	}

	p, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, p)

	metas := rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(500), metas[0].ID)
	require.Equal(t, manifest.RegionStateRunning, metas[0].State)
	require.Equal(t, 1, updateCalls)

	snapshot := mgr.RegionSnapshot()
	require.Len(t, snapshot, 1)
	meta, ok := snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRunning, meta.State)
	require.Zero(t, meta.Epoch.Version)

	updated := manifest.RegionMeta{
		ID:       500,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch: manifest.RegionEpoch{
			Version:     4,
			ConfVersion: 6,
		},
		State: manifest.RegionStateRunning,
		Peers: []manifest.PeerMeta{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 22},
		},
	}
	require.NoError(t, rs.UpdateRegion(updated))

	peerMeta := p.RegionMeta()
	require.NotNil(t, peerMeta)
	require.Equal(t, uint64(4), peerMeta.Epoch.Version)
	require.Len(t, peerMeta.Peers, 2)

	metas = rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(4), metas[0].Epoch.Version)

	metaByID, ok := rs.RegionMetaByID(500)
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRunning, metaByID.State)
	_, ok = rs.RegionMetaByID(999)
	require.False(t, ok)

	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, uint64(4), meta.Epoch.Version)
	require.Len(t, meta.Peers, 2)

	require.NoError(t, rs.UpdateRegionState(500, manifest.RegionStateRemoving))

	metas = rs.RegionMetas()
	require.Equal(t, manifest.RegionStateRemoving, metas[0].State)
	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRemoving, meta.State)

	rs.StopPeer(p.ID())
	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateRemoving, meta.State)

	require.NoError(t, rs.UpdateRegionState(500, manifest.RegionStateTombstone))

	metas = rs.RegionMetas()
	require.Equal(t, manifest.RegionStateTombstone, metas[0].State)
	snapshot = mgr.RegionSnapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, manifest.RegionStateTombstone, meta.State)

	err = rs.UpdateRegionState(500, manifest.RegionStateRunning)
	require.Error(t, err)

	require.NoError(t, rs.RemoveRegion(500))

	metas = rs.RegionMetas()
	require.Len(t, metas, 0)

	snapshot = mgr.RegionSnapshot()
	require.Len(t, snapshot, 0)
	require.Equal(t, 1, removeCalls)
	expectedStates := []manifest.RegionState{
		manifest.RegionStateRunning,
		manifest.RegionStateRunning,
		manifest.RegionStateRemoving,
		manifest.RegionStateRemoving,
		manifest.RegionStateTombstone,
	}
	require.Equal(t, expectedStates, states)
}
