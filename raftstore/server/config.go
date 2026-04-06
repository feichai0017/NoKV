package server

import (
	"time"

	NoKV "github.com/feichai0017/NoKV"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
)

// Config wires together the dependencies required to host one raftstore node
// and expose its shared gRPC surfaces.
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

const defaultRaftTickInterval = 100 * time.Millisecond
