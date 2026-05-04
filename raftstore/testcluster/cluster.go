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
	"github.com/feichai0017/NoKV/coordinator/catalog"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	workdirmode "github.com/feichai0017/NoKV/dbcore/mode"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	myraft "github.com/feichai0017/NoKV/raft"
	raftkv "github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/scheduler"
	schedulercoord "github.com/feichai0017/NoKV/scheduler/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	StoreID   uint64
	WorkDir   string
	DB        *NoKV.DB
	LocalMeta *localmeta.Store
	Server    *serverpkg.Node
}

type NodeConfig struct {
	AllowedModes      []workdirmode.Mode
	StartPeers        bool
	Scheduler         scheduler.Client
	HeartbeatInterval time.Duration
}

type Coordinator struct {
	addr    string
	lis     net.Listener
	server  *grpc.Server
	service *coordserver.Service
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

func StartNode(tb testing.TB, storeID uint64, dir string, allowedModes []workdirmode.Mode, startPeers bool) *Node {
	tb.Helper()
	return StartNodeWithConfig(tb, storeID, dir, NodeConfig{
		AllowedModes: allowedModes,
		StartPeers:   startPeers,
	})
}

func StartNodeWithConfig(tb testing.TB, storeID uint64, dir string, cfg NodeConfig) *Node {
	tb.Helper()
	localMeta, err := localmeta.OpenLocalStore(dir, nil)
	if err != nil {
		tb.Fatalf("open local meta: %v", err)
	}
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.RaftPointerSnapshot)
	if cfg.AllowedModes != nil {
		opt.AllowedModes = cfg.AllowedModes
	}
	db, err := NoKV.Open(opt)
	if err != nil {
		_ = localMeta.Close()
		tb.Fatalf("open node db: %v", err)
	}
	srv, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC:     db,
			Raft:     raftlog.NewDBLog(db),
			Snapshot: snapshotpkg.NewDBStore(db),
		},
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

func StartCoordinator(tb testing.TB) *Coordinator {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen coordinator: %v", err)
	}
	svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	grpcServer := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	return &Coordinator{
		addr:    lis.Addr().String(),
		lis:     lis,
		server:  grpcServer,
		service: svc,
	}
}

func (c *Coordinator) Addr() string {
	if c == nil {
		return ""
	}
	return c.addr
}

func (c *Coordinator) Close(tb testing.TB) {
	tb.Helper()
	if c == nil {
		return
	}
	if c.server != nil {
		c.server.Stop()
		c.server = nil
	}
	if c.lis != nil {
		if err := c.lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			tb.Fatalf("close coordinator listener: %v", err)
		}
		c.lis = nil
	}
}

func (c *Coordinator) PublishRootEvent(tb testing.TB, event rootevent.Event) {
	tb.Helper()
	if c == nil || c.service == nil {
		tb.Fatalf("publish root event: coordinator is not running")
	}
	resp, err := c.service.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		tb.Fatalf("publish root event %v: %v", event.Kind, err)
	}
	if resp == nil || !resp.GetAccepted() {
		tb.Fatalf("publish root event %v was not accepted", event.Kind)
	}
}

func (c *Coordinator) JoinStore(tb testing.TB, storeID uint64) {
	tb.Helper()
	c.PublishRootEvent(tb, rootevent.StoreJoined(storeID))
}

func (c *Coordinator) RetireStore(tb testing.TB, storeID uint64) {
	tb.Helper()
	c.PublishRootEvent(tb, rootevent.StoreRetired(storeID))
}

func NewScheduler(tb testing.TB, coordAddr string, timeout time.Duration) scheduler.Client {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cli, err := coordclient.NewGRPCClient(ctx, coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("dial coordinator client %s: %v", coordAddr, err)
	}
	return schedulercoord.NewClient(schedulercoord.Config{
		Coordinator: cli,
		Timeout:     timeout,
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
		storage, err := raftlog.NewDBLog(node.DB).Open(meta.ID, node.LocalMeta)
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

func (n *Node) Restart(tb testing.TB, allowedModes []workdirmode.Mode, startPeers bool) {
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

func FetchRuntimeStatus(tb testing.TB, ctx context.Context, addr string, regionID uint64) *adminpb.RegionRuntimeStatusResponse {
	tb.Helper()
	status, err := TryFetchRuntimeStatus(ctx, addr, regionID)
	if err != nil {
		tb.Fatalf("region runtime status: %v", err)
	}
	return status
}

func TryFetchRuntimeStatus(ctx context.Context, addr string, regionID uint64) (*adminpb.RegionRuntimeStatusResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial admin %s: %w", addr, err)
	}
	defer func() {
		_ = conn.Close()
	}()
	client := adminpb.NewRaftAdminClient(conn)
	status, err := client.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: regionID})
	if err != nil {
		return nil, err
	}
	return status, nil
}

func WaitForLeaderPeer(tb testing.TB, ctx context.Context, addr string, regionID, peerID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastStatus *adminpb.RegionRuntimeStatusResponse
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := TryPollRuntimeStatus(ctx, addr, regionID)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		lastStatus = status
		if status.GetKnown() && status.GetLeader() && status.GetLeaderPeerId() == peerID {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for leader peer %d on region %d at %s (last status=%s, last error=%v)", peerID, regionID, addr, formatRuntimeStatus(lastStatus), lastErr)
}

func WaitForHostedPeer(tb testing.TB, ctx context.Context, addr string, regionID, peerID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastStatus *adminpb.RegionRuntimeStatusResponse
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := TryPollRuntimeStatus(ctx, addr, regionID)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		lastStatus = status
		if status.GetKnown() && status.GetHosted() && status.GetLocalPeerId() == peerID && status.GetAppliedIndex() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for hosted peer %d on region %d at %s (last status=%s, last error=%v)", peerID, regionID, addr, formatRuntimeStatus(lastStatus), lastErr)
}

func WaitForNotHosted(tb testing.TB, ctx context.Context, addr string, regionID uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastStatus *adminpb.RegionRuntimeStatusResponse
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := TryPollRuntimeStatus(ctx, addr, regionID)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		lastStatus = status
		if !status.GetHosted() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for region %d at %s to become not hosted (last status=%s, last error=%v)", regionID, addr, formatRuntimeStatus(lastStatus), lastErr)
}

func FindLeader(tb testing.TB, ctx context.Context, regionID uint64, nodes ...*Node) (*Node, *adminpb.RegionRuntimeStatusResponse) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		node, status, err := TryFindLeader(ctx, regionID, nodes...)
		if err == nil {
			return node, status
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for any leader on region %d (last error=%v, status=%s)", regionID, lastErr, DumpStatus(tb, ctx, regionID, nodes...))
	return nil, nil
}

// TryFindLeader performs one best-effort leader scan without failing the caller's test.
func TryFindLeader(ctx context.Context, regionID uint64, nodes ...*Node) (*Node, *adminpb.RegionRuntimeStatusResponse, error) {
	var errs []error
	statuses := make([]string, 0, len(nodes))
	for _, node := range nodes {
		status, err := TryPollRuntimeStatus(ctx, node.Addr(), regionID)
		if err != nil {
			errs = append(errs, fmt.Errorf("store %d: %w", node.StoreID, err))
			continue
		}
		statuses = append(statuses, fmt.Sprintf("store=%d %s", node.StoreID, formatRuntimeStatus(status)))
		if status.GetKnown() && status.GetLeader() {
			return node, status, nil
		}
	}
	err := fmt.Errorf("no leader for region %d (status=%v)", regionID, statuses)
	if len(errs) > 0 {
		err = fmt.Errorf("%w: %w", err, errors.Join(errs...))
	}
	return nil, nil, err
}

// TryPollRuntimeStatus performs one bounded admin status RPC for polling loops.
func TryPollRuntimeStatus(ctx context.Context, addr string, regionID uint64) (*adminpb.RegionRuntimeStatusResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	return TryFetchRuntimeStatus(callCtx, addr, regionID)
}

func formatRuntimeStatus(status *adminpb.RegionRuntimeStatusResponse) string {
	if status == nil {
		return "<nil>"
	}
	return fmt.Sprintf("known=%v hosted=%v local=%d leader=%v leaderPeer=%d applied=%d", status.GetKnown(), status.GetHosted(), status.GetLocalPeerId(), status.GetLeader(), status.GetLeaderPeerId(), status.GetAppliedIndex())
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

func peerConfig(node *Node, meta localmeta.RegionMeta, peerID uint64, storage raftlog.PeerStorage) *peer.Config {
	snapshotStore := snapshotpkg.NewDBStore(node.DB)
	snapshotApply := func(payload []byte) (localmeta.RegionMeta, error) {
		result, err := snapshotStore.ImportSnapshot(payload)
		if err != nil {
			return localmeta.RegionMeta{}, err
		}
		return result.Meta.Region, nil
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
		Transport:      node.Server.Transport(),
		Apply:          raftkv.NewEntryApplier(node.DB),
		SnapshotExport: snapshotStore.ExportSnapshot,
		SnapshotApply:  snapshotApply,
		Storage:        storage,
		GroupID:        meta.ID,
		Region:         localmeta.CloneRegionMetaPtr(&meta),
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
		status, err := TryPollRuntimeStatus(ctx, node.Addr(), regionID)
		if err != nil {
			parts = append(parts, fmt.Sprintf("store=%d err=%v", node.StoreID, err))
			continue
		}
		parts = append(parts, fmt.Sprintf("store=%d %s", node.StoreID, formatRuntimeStatus(status)))
	}
	return fmt.Sprint(parts)
}

func WaitForSchedulerMode(tb testing.TB, node *Node, mode scheduler.Mode, degraded bool) {
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
