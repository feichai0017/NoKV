package testcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	pdadapter "github.com/feichai0017/NoKV/pd/adapter"
	pdclient "github.com/feichai0017/NoKV/pd/client"
	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	"github.com/feichai0017/NoKV/pd/tso"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	raftkv "github.com/feichai0017/NoKV/raftstore/kv"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/peer"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	StoreID   uint64
	WorkDir   string
	DB        *NoKV.DB
	LocalMeta *raftmeta.Store
	Server    *serverpkg.Server
}

type NodeConfig struct {
	AllowedModes      []raftmode.Mode
	StartPeers        bool
	Scheduler         storepkg.SchedulerClient
	HeartbeatInterval time.Duration
}

type PD struct {
	addr   string
	lis    net.Listener
	server *grpc.Server
}

func OpenStandaloneDB(tb testing.TB, dir string, tweak func(*NoKV.Options)) *NoKV.DB {
	tb.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	if tweak != nil {
		tweak(opt)
	}
	db, err := NoKV.Open(opt)
	if err != nil {
		tb.Fatalf("open standalone db: %v", err)
	}
	return db
}

func StartNode(tb testing.TB, storeID uint64, dir string, allowedModes []raftmode.Mode, startPeers bool) *Node {
	tb.Helper()
	return StartNodeWithConfig(tb, storeID, dir, NodeConfig{
		AllowedModes: allowedModes,
		StartPeers:   startPeers,
	})
}

func StartNodeWithConfig(tb testing.TB, storeID uint64, dir string, cfg NodeConfig) *Node {
	tb.Helper()
	localMeta, err := raftmeta.OpenLocalStore(dir, nil)
	if err != nil {
		tb.Fatalf("open local meta: %v", err)
	}
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	if cfg.AllowedModes != nil {
		opt.AllowedModes = cfg.AllowedModes
	}
	db, err := NoKV.Open(opt)
	if err != nil {
		_ = localMeta.Close()
		tb.Fatalf("open node db: %v", err)
	}
	srv, err := serverpkg.New(serverpkg.Config{
		Storage: serverpkg.Storage{MVCC: db, Raft: db.RaftLog()},
		Store: storepkg.Config{
			StoreID:           storeID,
			LocalMeta:         localMeta,
			Scheduler:         cfg.Scheduler,
			HeartbeatInterval: cfg.HeartbeatInterval,
		},
		Raft: myraft.Config{
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		TransportAddr: "127.0.0.1:0",
	})
	if err != nil {
		_ = db.Close()
		_ = localMeta.Close()
		tb.Fatalf("new server: %v", err)
	}
	node := &Node{StoreID: storeID, WorkDir: dir, DB: db, LocalMeta: localMeta, Server: srv}
	if cfg.StartPeers {
		StartPeers(tb, node)
	}
	return node
}

func StartPD(tb testing.TB) *PD {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen pd: %v", err)
	}
	svc := pdserver.NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	grpcServer := grpc.NewServer()
	pb.RegisterPDServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	return &PD{
		addr:   lis.Addr().String(),
		lis:    lis,
		server: grpcServer,
	}
}

func (pd *PD) Addr() string {
	if pd == nil {
		return ""
	}
	return pd.addr
}

func (pd *PD) Close(tb testing.TB) {
	tb.Helper()
	if pd == nil {
		return
	}
	if pd.server != nil {
		pd.server.Stop()
		pd.server = nil
	}
	if pd.lis != nil {
		if err := pd.lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			tb.Fatalf("close pd listener: %v", err)
		}
		pd.lis = nil
	}
}

func NewScheduler(tb testing.TB, pdAddr string, timeout time.Duration) storepkg.SchedulerClient {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cli, err := pdclient.NewGRPCClient(ctx, pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("dial pd client %s: %v", pdAddr, err)
	}
	return pdadapter.NewSchedulerClient(pdadapter.SchedulerClientConfig{
		PD:      cli,
		Timeout: timeout,
	})
}

func StartPeers(tb testing.TB, node *Node) {
	tb.Helper()
	snapshot := node.LocalMeta.Snapshot()
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
			if p.StoreID == node.StoreID {
				peerID = p.PeerID
				break
			}
		}
		if peerID == 0 {
			continue
		}
		storage, err := node.DB.RaftLog().Open(meta.ID, node.LocalMeta)
		if err != nil {
			tb.Fatalf("open peer storage: %v", err)
		}
		cfg := peerConfig(node, meta, peerID, storage)
		bootstrapPeers := make([]myraft.Peer, 0, len(meta.Peers))
		for _, p := range meta.Peers {
			bootstrapPeers = append(bootstrapPeers, myraft.Peer{ID: p.PeerID})
		}
		if _, err := node.Server.Store().StartPeer(cfg, bootstrapPeers); err != nil {
			tb.Fatalf("start peer %d: %v", peerID, err)
		}
	}
}

func (n *Node) Addr() string {
	return n.Server.Addr()
}

func (n *Node) WirePeers(peers map[uint64]string) {
	for peerID, addr := range peers {
		n.Server.Transport().SetPeer(peerID, addr)
	}
}

func (n *Node) BlockPeer(peerID uint64) {
	if n == nil || n.Server == nil || n.Server.Transport() == nil {
		return
	}
	n.Server.Transport().BlockPeer(peerID)
}

func (n *Node) UnblockPeer(peerID uint64) {
	if n == nil || n.Server == nil || n.Server.Transport() == nil {
		return
	}
	n.Server.Transport().UnblockPeer(peerID)
}

func (n *Node) Restart(tb testing.TB, allowedModes []raftmode.Mode, startPeers bool) {
	tb.Helper()
	workDir := n.WorkDir
	storeID := n.StoreID
	n.Close(tb)
	restarted := StartNode(tb, storeID, workDir, allowedModes, startPeers)
	*n = *restarted
}

func (n *Node) Close(tb testing.TB) {
	tb.Helper()
	if n == nil {
		return
	}
	if n.Server != nil {
		if err := n.Server.Close(); err != nil {
			tb.Fatalf("close server: %v", err)
		}
		n.Server = nil
	}
	if n.DB != nil {
		if err := n.DB.Close(); err != nil {
			tb.Fatalf("close db: %v", err)
		}
		n.DB = nil
	}
	if n.LocalMeta != nil {
		if err := n.LocalMeta.Close(); err != nil {
			tb.Fatalf("close local meta: %v", err)
		}
		n.LocalMeta = nil
	}
}

func FetchRuntimeStatus(tb testing.TB, ctx context.Context, addr string, regionID uint64) *pb.RegionRuntimeStatusResponse {
	tb.Helper()
	status, err := TryFetchRuntimeStatus(ctx, addr, regionID)
	if err != nil {
		tb.Fatalf("region runtime status: %v", err)
	}
	return status
}

func TryFetchRuntimeStatus(ctx context.Context, addr string, regionID uint64) (*pb.RegionRuntimeStatusResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial admin %s: %w", addr, err)
	}
	defer func() {
		_ = conn.Close()
	}()
	client := pb.NewRaftAdminClient(conn)
	status, err := client.RegionRuntimeStatus(ctx, &pb.RegionRuntimeStatusRequest{RegionId: regionID})
	if err != nil {
		return nil, err
	}
	return status, nil
}

func WaitForLeaderPeer(tb testing.TB, ctx context.Context, addr string, regionID, peerID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := FetchRuntimeStatus(tb, ctx, addr, regionID)
		if status.GetKnown() && status.GetLeader() && status.GetLeaderPeerId() == peerID {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for leader peer %d on region %d at %s", peerID, regionID, addr)
}

func WaitForHostedPeer(tb testing.TB, ctx context.Context, addr string, regionID, peerID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := FetchRuntimeStatus(tb, ctx, addr, regionID)
		if status.GetKnown() && status.GetHosted() && status.GetLocalPeerId() == peerID && status.GetAppliedIndex() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for hosted peer %d on region %d at %s", peerID, regionID, addr)
}

func WaitForNotHosted(tb testing.TB, ctx context.Context, addr string, regionID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := FetchRuntimeStatus(tb, ctx, addr, regionID)
		if !status.GetHosted() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for region %d at %s to become not hosted", regionID, addr)
}

func FindLeader(tb testing.TB, ctx context.Context, regionID uint64, nodes ...*Node) (*Node, *pb.RegionRuntimeStatusResponse) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			status := FetchRuntimeStatus(tb, ctx, node.Addr(), regionID)
			if status.GetKnown() && status.GetLeader() {
				return node, status
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for any leader on region %d", regionID)
	return nil, nil
}

func AssertValue(tb testing.TB, db *NoKV.DB, key, value []byte) {
	tb.Helper()
	entry, err := db.Get(key)
	if err != nil {
		tb.Fatalf("get key %q: %v", key, err)
	}
	if !bytes.Equal(entry.Value, value) {
		tb.Fatalf("value mismatch for key %q: got %q want %q", key, entry.Value, value)
	}
}

func peerConfig(node *Node, meta raftmeta.RegionMeta, peerID uint64, storage engine.PeerStorage) *peer.Config {
	var snapshotExport peer.SnapshotExportFunc
	if snapshotIO, ok := any(node.DB).(snapshotpkg.SnapshotIO); ok {
		snapshotExport = snapshotIO.ExportSnapshot
		return &peer.Config{
			RaftConfig: myraft.Config{
				ID:              peerID,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			Transport:      node.Server.Transport(),
			Apply:          raftkv.NewEntryApplier(node.DB),
			SnapshotExport: snapshotExport,
			SnapshotApply:  snapshotIO.InstallSnapshot,
			Storage:        storage,
			GroupID:        meta.ID,
			Region:         raftmeta.CloneRegionMetaPtr(&meta),
		}
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
		Transport: node.Server.Transport(),
		Apply:     raftkv.NewEntryApplier(node.DB),
		Storage:   storage,
		GroupID:   meta.ID,
		Region:    raftmeta.CloneRegionMetaPtr(&meta),
	}
}

func EnsureRegionPeer(tb testing.TB, node *Node, regionID, peerID uint64) {
	tb.Helper()
	status := FetchRuntimeStatus(tb, context.Background(), node.Addr(), regionID)
	if !status.GetKnown() || !status.GetHosted() || status.GetLocalPeerId() != peerID {
		tb.Fatalf("region %d on store %d not hosted as peer %d: %+v", regionID, node.StoreID, peerID, status)
	}
}

func DumpStatus(tb testing.TB, ctx context.Context, regionID uint64, nodes ...*Node) string {
	tb.Helper()
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		status := FetchRuntimeStatus(tb, ctx, node.Addr(), regionID)
		parts = append(parts, fmt.Sprintf("store=%d known=%v hosted=%v local=%d leader=%v leaderPeer=%d applied=%d", node.StoreID, status.GetKnown(), status.GetHosted(), status.GetLocalPeerId(), status.GetLeader(), status.GetLeaderPeerId(), status.GetAppliedIndex()))
	}
	return fmt.Sprint(parts)
}

func WaitForSchedulerMode(tb testing.TB, node *Node, mode storepkg.SchedulerMode, degraded bool) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := node.Server.Store().SchedulerStatus()
		if status.Mode == mode && status.Degraded == degraded {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for scheduler mode=%s degraded=%v on store %d (last=%+v)", mode, degraded, node.StoreID, node.Server.Store().SchedulerStatus())
}
