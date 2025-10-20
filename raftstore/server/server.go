package server

import (
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"google.golang.org/grpc"
)

// Config wires together the dependencies required to expose the TinyKv RPC
// service backed by a raftstore Store.
type Config struct {
	// DB provides the underlying storage engine.
	DB *NoKV.DB
	// Store describes how the raftstore should be constructed. StoreID must be
	// populated; Router, PeerFactory, and hooks are optional.
	Store store.Config
	// Raft provides the base raft configuration used when bootstrapping peers.
	// The Peer ID is populated per Region automatically.
	Raft myraft.Config
	// TransportAddr selects the listen address for the shared raft/TinyKv gRPC
	// server. An empty string defaults to 127.0.0.1:0.
	TransportAddr string
	// TransportOptions allows callers to override transport settings (TLS,
	// retry behaviour, etc.).
	TransportOptions []transport.GRPCOption
}

// Server bundles the components required to serve TinyKv RPCs backed by a
// raftstore Store.
type Server struct {
	db        *NoKV.DB
	store     *store.Store
	service   *kv.Service
	transport *transport.GRPCTransport
}

// New constructs a Server using the provided configuration.
func New(cfg Config) (*Server, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("raftstore/server: DB is required")
	}
	if cfg.Store.StoreID == 0 {
		return nil, fmt.Errorf("raftstore/server: StoreID must be set")
	}
	storeCfg := cfg.Store
	if storeCfg.CommandApplier == nil {
		storeCfg.CommandApplier = kv.NewApplier(cfg.DB)
	}
	router := storeCfg.Router
	if router == nil {
		router = store.NewRouter()
		storeCfg.Router = router
	}
	builder := storeCfg.PeerBuilder
	var transportRef transport.Transport
	if builder == nil {
		builder = func(meta manifest.RegionMeta) (*peer.Config, error) {
			if transportRef == nil {
				return nil, fmt.Errorf("raftstore/server: transport not initialised")
			}
			peerID := peerIDForStore(meta, storeCfg.StoreID)
			if peerID == 0 {
				return nil, fmt.Errorf("raftstore/server: store %d missing peer in region %d", storeCfg.StoreID, meta.ID)
			}
			raftCfg := cfg.Raft
			raftCfg.ID = peerID
			if raftCfg.ElectionTick == 0 {
				raftCfg.ElectionTick = 10
			}
			if raftCfg.HeartbeatTick == 0 {
				raftCfg.HeartbeatTick = 2
			}
			if raftCfg.MaxSizePerMsg == 0 {
				raftCfg.MaxSizePerMsg = 1 << 20
			}
			if raftCfg.MaxInflightMsgs == 0 {
				raftCfg.MaxInflightMsgs = 256
			}
			peerCfg := &peer.Config{
				RaftConfig: raftCfg,
				Transport:  transportRef,
				Apply:      kv.NewEntryApplier(cfg.DB),
				WAL:        cfg.DB.WAL(),
				Manifest:   cfg.DB.Manifest(),
				GroupID:    meta.ID,
				Region:     manifest.CloneRegionMetaPtr(&meta),
			}
			return peerCfg, nil
		}
	}
	storeCfg.PeerBuilder = builder

	st := store.NewStoreWithConfig(storeCfg)
	service := kv.NewService(st)

	var opts []transport.GRPCOption
	opts = append(opts, cfg.TransportOptions...)
	opts = append(opts, transport.WithServerRegistrar(func(reg grpc.ServiceRegistrar) {
		pb.RegisterTinyKvServer(reg, service)
	}))
	tr, err := transport.NewGRPCTransport(storeCfg.StoreID, cfg.TransportAddr, opts...)
	if err != nil {
		return nil, err
	}
	transportRef = tr
	tr.SetHandler(func(msg myraft.Message) error {
		return st.Step(msg)
	})

	return &Server{
		db:        cfg.DB,
		store:     st,
		service:   service,
		transport: tr,
	}, nil
}

// Addr returns the address TinyKv clients (and raft peers) should dial.
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

// Transport returns the shared raft/TinyKv gRPC transport.
func (s *Server) Transport() *transport.GRPCTransport {
	if s == nil {
		return nil
	}
	return s.transport
}

// Service returns the TinyKv RPC service implementation.
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
	if s.store != nil {
		s.store.Close()
	}
	return nil
}

func peerIDForStore(meta manifest.RegionMeta, storeID uint64) uint64 {
	for _, peer := range meta.Peers {
		if peer.StoreID == storeID {
			return peer.PeerID
		}
	}
	return 0
}
