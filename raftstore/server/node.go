package server

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/feichai0017/NoKV/raftstore/kv"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"google.golang.org/grpc"
)

var raftDebugLoggerOnce sync.Once

func enableRaftDebugLogging() {
	raftDebugLoggerOnce.Do(func() {
		logger := &myraft.DefaultLogger{Logger: log.New(os.Stderr, "raft ", log.LstdFlags)}
		logger.EnableTimestamps()
		logger.EnableDebug()
		myraft.SetLogger(logger)
	})
}

// Node hosts one raftstore store together with the shared gRPC transport,
// data-plane KV service, and admin service.
type Node struct {
	store     *store.Store
	transport *transport.GRPCTransport
	tickStop  chan struct{}
	tickWG    sync.WaitGroup
	tickEvery time.Duration
}

// NewNode constructs one raftstore node using the provided configuration.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Storage.MVCC == nil {
		return nil, fmt.Errorf("raftstore/server: MVCC storage is required")
	}
	snapshotBridge, ok := cfg.Storage.MVCC.(snapshotpkg.SnapshotStore)
	if !ok {
		return nil, fmt.Errorf("raftstore/server: MVCC storage must provide snapshot bridge")
	}
	if cfg.Store.StoreID == 0 {
		return nil, fmt.Errorf("raftstore/server: StoreID must be set")
	}
	storeCfg := cfg.Store
	if storeCfg.CommandApplier == nil {
		storeCfg.CommandApplier = kv.NewApplier(cfg.Storage.MVCC, nil)
	}
	router := storeCfg.Router
	if router == nil {
		router = store.NewRouter()
		storeCfg.Router = router
	}

	if cfg.EnableRaftDebugLog {
		enableRaftDebugLogging()
	}

	tr, err := transport.NewUnstartedGRPCTransport(storeCfg.StoreID, cfg.TransportAddr, cfg.TransportOptions...)
	if err != nil {
		return nil, err
	}

	builder := storeCfg.PeerBuilder
	if builder == nil {
		if cfg.Storage.Raft == nil {
			_ = tr.Close()
			return nil, fmt.Errorf("raftstore/server: raft log storage is required")
		}
		builder = defaultPeerBuilder(cfg.Storage, storeCfg.LocalMeta, storeCfg.StoreID, cfg.Raft, tr)
	}
	storeCfg.PeerBuilder = builder

	st := store.NewStore(storeCfg)
	kvService := kv.NewService(st)
	adminService := admin.NewServiceWithSnapshot(st, snapshotBridge)
	if err := tr.RegisterServer(func(reg grpc.ServiceRegistrar) {
		kvrpcpb.RegisterNoKVServer(reg, kvService)
		adminpb.RegisterRaftAdminServer(reg, adminService)
	}); err != nil {
		_ = tr.Close()
		return nil, err
	}
	tr.SetHandler(func(msg myraft.Message) error {
		return st.Step(msg)
	})
	if err := tr.Start(); err != nil {
		_ = tr.Close()
		return nil, err
	}
	clientAddr := storeCfg.ClientAddr
	if clientAddr == "" {
		clientAddr = tr.Addr()
	}
	raftAddr := storeCfg.RaftAddr
	if raftAddr == "" {
		raftAddr = clientAddr
	}
	st.SetAdvertiseAddrs(clientAddr, raftAddr)

	node := &Node{
		store:     st,
		transport: tr,
	}
	interval := cfg.RaftTickInterval
	if interval <= 0 {
		interval = defaultRaftTickInterval
	}
	node.startRaftTickLoop(interval)
	return node, nil
}

// Addr returns the address NoKV clients (and raft peers) should dial.
func (n *Node) Addr() string {
	if n == nil || n.transport == nil {
		return ""
	}
	return n.transport.Addr()
}

// Store exposes the underlying raftstore Store.
func (n *Node) Store() *store.Store {
	if n == nil {
		return nil
	}
	return n.store
}

// Transport returns the shared raft/NoKV gRPC transport.
func (n *Node) Transport() *transport.GRPCTransport {
	if n == nil {
		return nil
	}
	return n.transport
}

// Close stops the node transport. The caller remains responsible for closing
// the DB and store once outstanding operations are drained.
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	if n.transport != nil {
		if err := n.transport.Close(); err != nil {
			return err
		}
	}
	if n.tickStop != nil {
		close(n.tickStop)
		n.tickWG.Wait()
		n.tickStop = nil
	}
	if n.store != nil {
		n.store.Close()
	}
	return nil
}
