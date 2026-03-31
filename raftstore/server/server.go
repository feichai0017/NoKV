package server

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	adminsvc "github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/feichai0017/NoKV/raftstore/kv"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"google.golang.org/grpc"
)

// Config wires together the dependencies required to expose the NoKV RPC
// service backed by a raftstore Store.
type Config struct {
	// Storage provides the narrow storage capabilities needed by raftstore.
	Storage Storage
	// Store describes how the raftstore should be constructed. StoreID must be
	// populated; Router and PeerBuilder are optional.
	Store store.Config
	// Raft provides the base raft configuration used when bootstrapping peers.
	// The Peer ID is populated per Region automatically.
	Raft myraft.Config
	// TransportAddr selects the listen address for the shared raft/NoKV gRPC
	// server. An empty string defaults to 127.0.0.1:0.
	TransportAddr string
	// TransportOptions allows callers to override transport settings (TLS,
	// retry behaviour, etc.).
	TransportOptions []transport.GRPCOption
	// RaftTickInterval controls how often the server calls BroadcastTick on the
	// store router. When zero or negative a default of 100ms is used.
	RaftTickInterval time.Duration
	// EnableRaftDebugLog enables verbose etcd/raft debug logging so replication/apply traces are emitted.
	EnableRaftDebugLog bool
}

// Storage captures the engine capabilities raftstore needs.
type Storage struct {
	MVCC NoKV.MVCCStore
	Raft NoKV.RaftLog
}

// Server bundles the components required to serve NoKV RPCs backed by a
// raftstore Store.
type Server struct {
	store     *store.Store
	service   *kv.Service
	transport *transport.GRPCTransport
	tickStop  chan struct{}
	tickWG    sync.WaitGroup
	tickEvery time.Duration
}

const defaultRaftTickInterval = 100 * time.Millisecond

var raftDebugLoggerOnce sync.Once

// New constructs a Server using the provided configuration.
func New(cfg Config) (*Server, error) {
	if cfg.Storage.MVCC == nil {
		return nil, fmt.Errorf("raftstore/server: MVCC storage is required")
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
	service := kv.NewService(st)
	adminService := adminsvc.NewService(st)
	if src, ok := cfg.Storage.MVCC.(snapshotpkg.Source); ok {
		if sstSink, ok := cfg.Storage.MVCC.(snapshotpkg.SSTSink); ok {
			if optProvider, ok := cfg.Storage.MVCC.(interface{ SSTOptions() *lsm.Options }); ok {
				adminService = adminsvc.NewServiceWithSnapshotIO(st, src, sstSink, optProvider.SSTOptions(), nil)
			}
		}
	}
	if err := tr.RegisterServer(func(reg grpc.ServiceRegistrar) {
		pb.RegisterNoKVServer(reg, service)
		pb.RegisterRaftAdminServer(reg, adminService)
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

	srv := &Server{
		store:     st,
		service:   service,
		transport: tr,
	}
	interval := cfg.RaftTickInterval
	if interval <= 0 {
		interval = defaultRaftTickInterval
	}
	srv.startRaftTickLoop(interval)
	return srv, nil
}

func defaultPeerBuilder(storage Storage, localMeta *raftmeta.Store, storeID uint64, baseRaft myraft.Config, tr transport.Transport) store.PeerBuilder {
	return func(meta raftmeta.RegionMeta) (*peer.Config, error) {
		var peerID uint64
		for _, p := range meta.Peers {
			if p.StoreID == storeID {
				peerID = p.PeerID
				break
			}
		}
		if peerID == 0 {
			return nil, fmt.Errorf("raftstore/server: store %d missing peer in region %d", storeID, meta.ID)
		}
		peerStorage, err := storage.Raft.Open(meta.ID, localMeta)
		if err != nil {
			return nil, fmt.Errorf("raftstore/server: open peer storage for region %d: %w", meta.ID, err)
		}
		var snapshotExport peer.SnapshotExportFunc
		var snapshotApply peer.SnapshotApplyFunc
		if payloadIO, ok := storage.MVCC.(snapshotpkg.PayloadIO); ok {
			snapshotExport = payloadIO.ExportSSTPayload
			snapshotApply = payloadIO.ImportSSTPayload
		}
		return &peer.Config{
			RaftConfig:     defaultRaftConfig(baseRaft, peerID),
			Transport:      tr,
			Apply:          kv.NewEntryApplier(storage.MVCC),
			SnapshotExport: snapshotExport,
			SnapshotApply:  snapshotApply,
			Storage:        peerStorage,
			GroupID:        meta.ID,
			Region:         raftmeta.CloneRegionMetaPtr(&meta),
		}, nil
	}
}

func defaultRaftConfig(base myraft.Config, peerID uint64) myraft.Config {
	base.ID = peerID
	if base.ElectionTick == 0 {
		base.ElectionTick = 10
	}
	if base.HeartbeatTick == 0 {
		base.HeartbeatTick = 2
	}
	if base.MaxSizePerMsg == 0 {
		base.MaxSizePerMsg = 1 << 20
	}
	if base.MaxInflightMsgs == 0 {
		base.MaxInflightMsgs = 256
	}
	return base
}

// Addr returns the address NoKV clients (and raft peers) should dial.
func (s *Server) Addr() string {
	if s == nil || s.transport == nil {
		return ""
	}
	return s.transport.Addr()
}

// Store exposes the underlying raftstore Store.
func (s *Server) Store() *store.Store {
	if s == nil {
		return nil
	}
	return s.store
}

// Transport returns the shared raft/NoKV gRPC transport.
func (s *Server) Transport() *transport.GRPCTransport {
	if s == nil {
		return nil
	}
	return s.transport
}

// Service returns the NoKV RPC service implementation.
func (s *Server) Service() *kv.Service {
	if s == nil {
		return nil
	}
	return s.service
}

// Close stops the server transport. The caller remains responsible for closing
// the DB and store once outstanding operations are drained.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	if s.transport != nil {
		if err := s.transport.Close(); err != nil {
			return err
		}
	}
	if s.tickStop != nil {
		close(s.tickStop)
		s.tickWG.Wait()
		s.tickStop = nil
	}
	if s.store != nil {
		s.store.Close()
	}
	return nil
}

func enableRaftDebugLogging() {
	raftDebugLoggerOnce.Do(func() {
		logger := &myraft.DefaultLogger{Logger: log.New(os.Stderr, "raft ", log.LstdFlags)}
		logger.EnableTimestamps()
		logger.EnableDebug()
		myraft.SetLogger(logger)
	})
}

func (s *Server) startRaftTickLoop(interval time.Duration) {
	if s == nil || interval <= 0 {
		return
	}
	if s.store == nil {
		return
	}
	router := s.store.Router()
	if router == nil {
		return
	}
	if s.tickStop != nil {
		return
	}
	s.tickEvery = interval
	s.tickStop = make(chan struct{})
	s.tickWG.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = router.BroadcastTick()
			case <-s.tickStop:
				return
			}
		}
	})
}
