package server

import (
	"time"

	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
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
	// TransportAddr selects the listen address for the shared raft/StoreKV gRPC
	// server. An empty string defaults to 127.0.0.1:0.
	TransportAddr string
	// TransportOptions allows callers to override transport settings (TLS,
	// retry behaviour, etc.).
	TransportOptions []transport.GRPCOption
	// RaftTickInterval controls how often the server calls BroadcastTick on the
	// store router. When zero or negative a default of 100ms is used.
	RaftTickInterval time.Duration
	// MVCCMaintenance enables replicated MVCC maintenance for cluster-mode
	// stores. A zero Interval disables the worker.
	MVCCMaintenance MVCCMaintenanceConfig
	// MVCCGCPlan enables the read-only MVCC GC planner for raftstore runtimes.
	// The planner records deletion candidates for stats; destructive cleanup is
	// owned by MVCCMaintenance.
	MVCCGCPlan MVCCGCPlanConfig
	// EnableRaftDebugLog enables verbose etcd/raft debug logging so replication/apply traces are emitted.
	EnableRaftDebugLog bool
	// PerasWitness enables StoreKV's experimental fsmeta Peras witness RPCs.
	// Nil keeps the wire surface registered but returns FailedPrecondition.
	PerasWitness kv.PerasWitness
	// PerasAuthorityTable enables storage-side admission fencing for ordinary
	// fsmeta writes that target an active Peras authority. Nil leaves raftstore
	// apply behaviour unchanged.
	PerasAuthorityTable *runtimeperas.ActiveAuthorities
}

// MVCCGCPlanConfig describes the read-only MVCC GC planner owned by raftstore
// server assembly.
type MVCCGCPlanConfig struct {
	Interval time.Duration

	SafePoint func() uint64
	Retention func() rootstate.SnapshotRetentionIndex
	Mount     storemvcc.MountResolver
}

// MVCCMaintenanceConfig describes replicated MVCC maintenance owned by the
// raftstore server. Destructive operations always go through raft proposals.
type MVCCMaintenanceConfig struct {
	Interval time.Duration
	Timeout  time.Duration

	SafePoint   func() uint64
	CurrentTs   func() uint64
	CurrentTime func() uint64
	Retention   func() rootstate.SnapshotRetentionIndex
	Mount       storemvcc.MountResolver

	Apply        storemvcc.ApplyOptions
	ResolveLocks storemvcc.ResolveLocksOptions
	LockResolver storemvcc.LockResolver

	RunOrphanDefaults bool
	OrphanDefaults    storemvcc.OrphanDefaultOptions
}

// Storage captures the engine capabilities raftstore needs.
type Storage struct {
	MVCC     txnstore.Store
	Raft     RaftLog
	Snapshot snapshotpkg.SnapshotStore
}

type RaftLog interface {
	Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error)
}

const defaultRaftTickInterval = 100 * time.Millisecond
