package store

import (
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/stretchr/testify/require"
)

func TestStorePeersSnapshot(t *testing.T) {
	router := NewRouter()
	rs := NewStore(Config{
		Router: router,
	})

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              2,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       200,
			StartKey: []byte("c"),
			EndKey:   []byte("d"),
		},
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)

	handles := rs.Peers()
	require.Len(t, handles, 1)
	require.NotNil(t, handles[0].Region)
	handles[0].Region.StartKey[0] = 'x'
	meta := peer.RegionMeta()
	require.Equal(t, byte('c'), meta.StartKey[0])

	rs.StopPeer(peer.ID())
	require.Empty(t, rs.Peers())
}

func TestStorePersistsRegionMetadata(t *testing.T) {
	dir := t.TempDir()
	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = metaStore.Close() })

	rs := NewStore(Config{
		LocalMeta: metaStore,
	})

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              3,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
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
	require.Equal(t, localmeta.RegionStateRunning, metas[0].State)
	metricsSnap := rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(1), metricsSnap.Total)
	require.Equal(t, uint64(1), metricsSnap.Running)

	snapshot := metaStore.Snapshot()
	require.Len(t, snapshot, 1)
	meta, ok := snapshot[500]
	require.True(t, ok)
	require.Equal(t, localmeta.RegionStateRunning, meta.State)
	require.Zero(t, meta.Epoch.Version)

	updated := localmeta.RegionMeta{
		ID:       500,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch: localmeta.RegionEpoch{
			Version:     4,
			ConfVersion: 6,
		},
		State: localmeta.RegionStateRunning,
		Peers: []localmeta.PeerMeta{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 22},
		},
	}
	require.NoError(t, rs.applyRegionMeta(updated))

	peerMeta := p.RegionMeta()
	require.NotNil(t, peerMeta)
	require.Equal(t, uint64(4), peerMeta.Epoch.Version)
	require.Len(t, peerMeta.Peers, 2)

	metas = rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(4), metas[0].Epoch.Version)

	metaByID, ok := rs.RegionMetaByID(500)
	require.True(t, ok)
	require.Equal(t, localmeta.RegionStateRunning, metaByID.State)
	_, ok = rs.RegionMetaByID(999)
	require.False(t, ok)

	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, uint64(4), meta.Epoch.Version)
	require.Len(t, meta.Peers, 2)

	require.NoError(t, rs.applyRegionState(500, localmeta.RegionStateRemoving))

	metas = rs.RegionMetas()
	require.Equal(t, localmeta.RegionStateRemoving, metas[0].State)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(1), metricsSnap.Total)
	require.Equal(t, uint64(1), metricsSnap.Removing)
	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, localmeta.RegionStateRemoving, meta.State)

	rs.StopPeer(p.ID())
	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, localmeta.RegionStateRemoving, meta.State)

	require.NoError(t, rs.applyRegionState(500, localmeta.RegionStateTombstone))

	metas = rs.RegionMetas()
	require.Equal(t, localmeta.RegionStateTombstone, metas[0].State)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(1), metricsSnap.Total)
	require.Equal(t, uint64(1), metricsSnap.Tombstone)
	snapshot = metaStore.Snapshot()
	meta, ok = snapshot[500]
	require.True(t, ok)
	require.Equal(t, localmeta.RegionStateTombstone, meta.State)

	err = rs.applyRegionState(500, localmeta.RegionStateRunning)
	require.Error(t, err)

	require.NoError(t, rs.applyRegionRemoval(500))

	metas = rs.RegionMetas()
	require.Len(t, metas, 0)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Zero(t, metricsSnap.Total)

	snapshot = metaStore.Snapshot()
	require.Len(t, snapshot, 0)

	child := localmeta.RegionMeta{
		ID:       600,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}
	parent := localmeta.RegionMeta{
		ID:       500,
		StartKey: []byte("k"),
		EndKey:   []byte("m"),
		State:    localmeta.RegionStateRunning,
	}
	require.NoError(t, rs.applyRegionMeta(parent))
	require.NoError(t, rs.applyRegionMeta(child))

	metas = rs.RegionMetas()
	require.Len(t, metas, 2)
	metricsSnap = rs.RegionMetrics().Snapshot()
	require.Equal(t, uint64(2), metricsSnap.Total)
	require.Equal(t, uint64(2), metricsSnap.Running)
}

func TestStoreLoadsLocalMetaSnapshotWithoutScheduler(t *testing.T) {
	dir := t.TempDir()
	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = metaStore.Close() })

	require.NoError(t, metaStore.SaveRegion(localmeta.RegionMeta{
		ID:       901,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		State:    localmeta.RegionStateRunning,
		Epoch:    localmeta.RegionEpoch{Version: 2, ConfVersion: 3},
		Peers:    []localmeta.PeerMeta{{StoreID: 1, PeerID: 11}},
	}))
	require.NoError(t, metaStore.SaveRegion(localmeta.RegionMeta{
		ID:       902,
		StartKey: []byte("m"),
		EndKey:   nil,
		State:    localmeta.RegionStateRunning,
		Epoch:    localmeta.RegionEpoch{Version: 5, ConfVersion: 8},
		Peers:    []localmeta.PeerMeta{{StoreID: 1, PeerID: 12}},
	}))

	rs := NewStore(Config{
		StoreID:   1,
		LocalMeta: metaStore,
	})
	defer rs.Close()

	metas := rs.RegionMetas()
	require.Len(t, metas, 2)

	meta901, ok := rs.RegionMetaByID(901)
	require.True(t, ok)
	require.Equal(t, []byte("a"), meta901.StartKey)
	require.Equal(t, uint64(2), meta901.Epoch.Version)

	meta902, ok := rs.RegionMetaByID(902)
	require.True(t, ok)
	require.Equal(t, []byte("m"), meta902.StartKey)
	require.Equal(t, uint64(8), meta902.Epoch.ConfVersion)
}
