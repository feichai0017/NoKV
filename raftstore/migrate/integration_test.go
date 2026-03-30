package migrate

import (
	"context"
	"slices"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	raftkv "github.com/feichai0017/NoKV/raftstore/kv"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/peer"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
)

type integrationNode struct {
	storeID   uint64
	workDir   string
	db        *NoKV.DB
	localMeta *raftmeta.Store
	srv       *serverpkg.Server
}

func TestMigrationFlowEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := openIntegrationDB(t, seedDir, nil, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	smallKey := []byte("small-key")
	smallValue := []byte("small-value")
	largeKey := []byte("large-key")
	largeValue := make([]byte, 4096)
	for i := range largeValue {
		largeValue[i] = byte('a' + (i % 23))
	}
	require.NoError(t, standalone.Set(smallKey, smallValue))
	require.NoError(t, standalone.Set(largeKey, largeValue))
	require.NoError(t, standalone.Close())

	plan, err := BuildPlan(seedDir)
	require.NoError(t, err)
	require.True(t, plan.Eligible)

	_, err = Init(InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 1, PeerID: 101})
	require.NoError(t, err)

	seed := startIntegrationNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target2 := startIntegrationNode(t, 2, t.TempDir(), nil, false)
	target3 := startIntegrationNode(t, 3, t.TempDir(), nil, false)
	defer closeIntegrationNode(t, target3)
	defer closeIntegrationNode(t, target2)
	defer closeIntegrationNode(t, seed)

	wireIntegrationPeers(seed, map[uint64]string{201: target2.srv.Addr(), 301: target3.srv.Addr()})
	wireIntegrationPeers(target2, map[uint64]string{101: seed.srv.Addr(), 301: target3.srv.Addr()})
	wireIntegrationPeers(target3, map[uint64]string{101: seed.srv.Addr(), 201: target2.srv.Addr()})

	waitForLeaderPeerStatus(t, ctx, seed.srv.Addr(), 1, 101)

	expandResult, err := Expand(ctx, ExpandConfig{
		Addr:         seed.srv.Addr(),
		RegionID:     1,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 201, TargetAdminAddr: target2.srv.Addr()},
			{StoreID: 3, PeerID: 301, TargetAdminAddr: target3.srv.Addr()},
		},
	})
	require.NoError(t, err)
	require.Len(t, expandResult.Results, 2)
	for _, step := range expandResult.Results {
		require.True(t, step.TargetHosted)
		require.Greater(t, step.TargetAppliedIdx, uint64(0))
	}

	assertDBValue(t, target2.db, smallKey, smallValue)
	assertDBValue(t, target2.db, largeKey, largeValue)
	assertDBValue(t, target3.db, smallKey, smallValue)
	assertDBValue(t, target3.db, largeKey, largeValue)

	transferResult, err := TransferLeader(ctx, TransferLeaderConfig{
		Addr:            seed.srv.Addr(),
		TargetAdminAddr: target2.srv.Addr(),
		RegionID:        1,
		PeerID:          201,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, transferResult.TargetLeader)
	require.Equal(t, uint64(201), transferResult.LeaderPeerID)

	removeResult, err := RemovePeer(ctx, RemovePeerConfig{
		Addr:            target2.srv.Addr(),
		TargetAdminAddr: seed.srv.Addr(),
		RegionID:        1,
		PeerID:          101,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.False(t, removeResult.TargetHosted)

	leaderStatus := fetchRuntimeStatus(t, ctx, target2.srv.Addr(), 1)
	require.True(t, leaderStatus.GetKnown())
	require.Len(t, leaderStatus.GetRegion().GetPeers(), 2)
	require.False(t, regionContainsPeer(leaderStatus.GetRegion(), 101))

	seedStatus := fetchRuntimeStatus(t, ctx, seed.srv.Addr(), 1)
	require.False(t, seedStatus.GetHosted())
	assertDBValue(t, target2.db, largeKey, largeValue)
}

func TestExpandedPeerRestartPreservesRegionAndData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := openIntegrationDB(t, seedDir, nil, func(opt *NoKV.Options) {
		opt.ValueThreshold = 8
	})
	key := []byte("restart-key")
	value := make([]byte, 2048)
	for i := range value {
		value[i] = byte('k' + (i % 7))
	}
	require.NoError(t, standalone.Set(key, value))
	require.NoError(t, standalone.Close())

	_, err := Init(InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 9, PeerID: 101})
	require.NoError(t, err)

	seed := startIntegrationNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	targetDir := t.TempDir()
	target := startIntegrationNode(t, 2, targetDir, nil, false)
	defer closeIntegrationNode(t, seed)
	wireIntegrationPeers(seed, map[uint64]string{201: target.srv.Addr()})
	wireIntegrationPeers(target, map[uint64]string{101: seed.srv.Addr()})
	waitForLeaderPeerStatus(t, ctx, seed.srv.Addr(), 9, 101)

	_, err = Expand(ctx, ExpandConfig{
		Addr:         seed.srv.Addr(),
		RegionID:     9,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.srv.Addr()}},
	})
	require.NoError(t, err)
	assertDBValue(t, target.db, key, value)

	closeIntegrationNode(t, target)
	target = startIntegrationNode(t, 2, targetDir, nil, true)
	defer closeIntegrationNode(t, target)
	wireIntegrationPeers(seed, map[uint64]string{201: target.srv.Addr()})
	wireIntegrationPeers(target, map[uint64]string{101: seed.srv.Addr()})
	waitForHostedPeer(t, ctx, target.srv.Addr(), 9, 201)
	assertDBValue(t, target.db, key, value)
}

func openIntegrationDB(t *testing.T, dir string, allowedModes []raftmode.Mode, tweak func(*NoKV.Options)) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	if tweak != nil {
		tweak(opt)
	}
	if allowedModes != nil {
		opt.AllowedModes = allowedModes
	}
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db
}

func startIntegrationNode(t *testing.T, storeID uint64, dir string, allowedModes []raftmode.Mode, startPeers bool) *integrationNode {
	t.Helper()
	localMeta, err := raftmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	if allowedModes != nil {
		opt.AllowedModes = allowedModes
	}
	db, err := NoKV.Open(opt)
	require.NoError(t, err)

	srv, err := serverpkg.New(serverpkg.Config{
		Storage: serverpkg.Storage{MVCC: db, Raft: db.RaftLog()},
		Store:   storepkg.Config{StoreID: storeID, LocalMeta: localMeta},
		Raft: myraft.Config{
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)

	node := &integrationNode{storeID: storeID, workDir: dir, db: db, localMeta: localMeta, srv: srv}
	if startPeers {
		startIntegrationPeers(t, node)
	}
	return node
}

func startIntegrationPeers(t *testing.T, node *integrationNode) {
	t.Helper()
	snapshot := node.localMeta.Snapshot()
	ids := make([]uint64, 0, len(snapshot))
	for id := range snapshot {
		if id != 0 {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		meta := snapshot[id]
		var peerID uint64
		for _, p := range meta.Peers {
			if p.StoreID == node.storeID {
				peerID = p.PeerID
				break
			}
		}
		if peerID == 0 {
			continue
		}
		storage, err := node.db.RaftLog().Open(meta.ID, node.localMeta)
		require.NoError(t, err)
		cfg := serverPeerConfig(node, meta, peerID, storage)
		bootstrapPeers := make([]myraft.Peer, 0, len(meta.Peers))
		for _, p := range meta.Peers {
			bootstrapPeers = append(bootstrapPeers, myraft.Peer{ID: p.PeerID})
		}
		_, err = node.srv.Store().StartPeer(cfg, bootstrapPeers)
		require.NoError(t, err)
	}
}

func serverPeerConfig(node *integrationNode, meta raftmeta.RegionMeta, peerID uint64, storage engine.PeerStorage) *peer.Config {
	var snapshotExport peer.SnapshotExportFunc
	if src, ok := any(node.db).(interface {
		NoKV.MVCCStore
		MaterializeInternalEntry(src *entrykv.Entry) (*entrykv.Entry, error)
	}); ok {
		snapshotExport = func(region raftmeta.RegionMeta) ([]byte, error) {
			payload, _, err := snapshotpkg.ExportPayload(src, region)
			return payload, err
		}
	}
	snapshotApply := func(payload []byte) (raftmeta.RegionMeta, error) {
		result, err := snapshotpkg.ImportPayload(node.db, payload)
		if err != nil {
			return raftmeta.RegionMeta{}, err
		}
		return result.Manifest.Region, nil
	}
	return &peer.Config{
		RaftConfig: myraft.Config{
			ID:              peerID,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:      node.srv.Transport(),
		Apply:          raftkv.NewEntryApplier(node.db),
		SnapshotExport: snapshotExport,
		SnapshotApply:  snapshotApply,
		Storage:        storage,
		GroupID:        meta.ID,
		Region:         raftmeta.CloneRegionMetaPtr(&meta),
	}
}

func wireIntegrationPeers(node *integrationNode, peers map[uint64]string) {
	for peerID, addr := range peers {
		node.srv.Transport().SetPeer(peerID, addr)
	}
}

func closeIntegrationNode(t *testing.T, node *integrationNode) {
	t.Helper()
	if node == nil {
		return
	}
	if node.srv != nil {
		require.NoError(t, node.srv.Close())
		node.srv = nil
	}
	if node.db != nil {
		require.NoError(t, node.db.Close())
		node.db = nil
	}
	if node.localMeta != nil {
		require.NoError(t, node.localMeta.Close())
		node.localMeta = nil
	}
}

func fetchRuntimeStatus(t *testing.T, ctx context.Context, addr string, regionID uint64) *pb.RegionRuntimeStatusResponse {
	t.Helper()
	client, closeFn, err := defaultDial(ctx, addr)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeFn()) }()
	status, err := client.RegionRuntimeStatus(ctx, &pb.RegionRuntimeStatusRequest{RegionId: regionID})
	require.NoError(t, err)
	return status
}

func waitForLeaderPeerStatus(t *testing.T, ctx context.Context, addr string, regionID, peerID uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := fetchRuntimeStatus(t, ctx, addr, regionID)
		return status.GetKnown() && status.GetLeader() && status.GetLeaderPeerId() == peerID
	}, 5*time.Second, 20*time.Millisecond)
}

func waitForHostedPeer(t *testing.T, ctx context.Context, addr string, regionID, peerID uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := fetchRuntimeStatus(t, ctx, addr, regionID)
		return status.GetKnown() && status.GetHosted() && status.GetLocalPeerId() == peerID && status.GetAppliedIndex() > 0
	}, 5*time.Second, 20*time.Millisecond)
}

func assertDBValue(t *testing.T, db *NoKV.DB, key, value []byte) {
	t.Helper()
	entry, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, entry.Value)
}
