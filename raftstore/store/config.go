package store

import (
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

// PeerFactory constructs raft peers for the store. It mirrors TinyKV's ability
// to plug customised peer state machines (e.g. learners, schedulers) while
// keeping the store orchestration generic.
type PeerFactory func(*peer.Config) (*peer.Peer, error)

// PeerBuilder constructs peer configuration for the provided region metadata.
// It allows the store to spawn new peers for splits without external callers
// wiring the configuration manually.
type PeerBuilder func(meta manifest.RegionMeta) (*peer.Config, error)

// LifecycleHooks exposes callbacks triggered when peers are started or
// stopped. The hooks allow tests and higher-level components to mirror
// TinyKV's raftstore design, where the store notifies schedulers about region
// lifecycle events.
type LifecycleHooks struct {
	OnPeerStart func(*peer.Peer)
	OnPeerStop  func(*peer.Peer)
}

// RegionHooks exposes callbacks triggered when region metadata changes or is
// removed from the store catalog.
type RegionHooks struct {
	OnRegionUpdate func(manifest.RegionMeta)
	OnRegionRemove func(uint64)
}

// Config configures Store construction. Only the Router field is mandatory;
// factory and hooks default to sensible values when omitted.
type Config struct {
	Router             *Router
	PeerFactory        PeerFactory
	PeerBuilder        PeerBuilder
	Hooks              LifecycleHooks
	RegionHooks        RegionHooks
	Manifest           *manifest.Manager
	Scheduler          scheduler.RegionSink
	HeartbeatInterval  time.Duration
	StoreID            uint64
	Planner            scheduler.Planner
	OperationQueueSize int
	OperationCooldown  time.Duration
	OperationInterval  time.Duration
	OperationBurst     int
	OperationObserver  func(scheduler.Operation)
}
