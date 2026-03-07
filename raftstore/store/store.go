package store

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

// Store hosts a collection of peers and provides helpers inspired by
// TinyKV's raftstore::Store structure. It wires peers to the router, exposes
// lifecycle hooks, and allows higher layers (RPC, schedulers, tests) to drive
// ticks or proposals without needing to keep global peer maps themselves.
type Store struct {
	mu             sync.RWMutex
	router         *Router
	peers          *peerSet
	peerFactory    PeerFactory
	peerBuilder    PeerBuilder
	hooks          LifecycleHooks
	regionMetrics  *metrics.RegionMetrics
	manifest       *manifest.Manager
	regions        *regionManager
	scheduler      scheduler.RegionSink
	storeID        uint64
	operationHook  func(scheduler.Operation)
	commandApplier func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)
	command        *commandPipeline
	commandTimeout time.Duration
	operations     *operationScheduler
	heartbeat      *heartbeatLoop
}

type operationKey struct {
	region uint64
	typeID scheduler.OperationType
}

// PeerHandle is a lightweight view of a peer registered with the store. It is
// designed for diagnostics and scheduling components so they can iterate over
// the cluster topology without touching the internal map directly.
type PeerHandle struct {
	ID     uint64
	Peer   *peer.Peer
	Region *manifest.RegionMeta
}

// RegionSnapshot provides an external view of the tracked Region metadata.
type RegionSnapshot struct {
	Regions []manifest.RegionMeta `json:"regions"`
}

// NewStore creates a Store with the provided router. When router is nil a new
// instance is allocated implicitly so callers can skip the explicit
// construction in tests.
func NewStore(router *Router) *Store {
	return NewStoreWithConfig(Config{Router: router})
}

// NewStoreWithConfig allows callers to supply a custom PeerFactory and
// LifecycleHooks when creating a store. This mirrors TinyKV's configurable
// raftstore bootstrap pipeline where schedulers wire themselves into peer
// lifecycle events.
func NewStoreWithConfig(cfg Config) *Store {
	router := cfg.Router
	if router == nil {
		router = NewRouter()
	}
	factory := cfg.PeerFactory
	if factory == nil {
		factory = peer.NewPeer
	}
	regionMetrics := metrics.NewRegionMetrics()
	hookChain := []RegionHooks{regionMetrics.Hooks()}
	if cfg.Scheduler != nil {
		hookChain = append(hookChain, RegionHooks{
			OnRegionUpdate: cfg.Scheduler.SubmitRegionHeartbeat,
			OnRegionRemove: cfg.Scheduler.RemoveRegion,
		})
	}
	hookChain = append(hookChain, cfg.RegionHooks)
	combinedHooks := mergeRegionHooks(hookChain...)
	// Scheduler is the single injected control-plane object. When it also
	// implements Planner, store will consume planner output from the same source.
	// Otherwise planner is disabled.
	var planner scheduler.Planner
	if inferred, ok := cfg.Scheduler.(scheduler.Planner); ok {
		planner = inferred
	}
	queueSize := max(cfg.OperationQueueSize, 0)
	operationCooldown := max(cfg.OperationCooldown, 0)
	if operationCooldown == 0 {
		operationCooldown = 5 * time.Second
	}
	operationInterval := cfg.OperationInterval
	if operationInterval <= 0 {
		operationInterval = cfg.HeartbeatInterval
	}
	if operationInterval <= 0 {
		operationInterval = 200 * time.Millisecond
	}
	operationBurst := max(cfg.OperationBurst, 0)
	if operationBurst == 0 {
		operationBurst = 4
	}
	commandTimeout := cfg.CommandTimeout
	if commandTimeout <= 0 {
		commandTimeout = 3 * time.Second
	}
	s := &Store{
		router:         router,
		peers:          newPeerSet(),
		peerFactory:    factory,
		peerBuilder:    cfg.PeerBuilder,
		hooks:          cfg.Hooks,
		regionMetrics:  regionMetrics,
		manifest:       cfg.Manifest,
		scheduler:      cfg.Scheduler,
		storeID:        cfg.StoreID,
		operationHook:  cfg.OperationObserver,
		commandApplier: cfg.CommandApplier,
		commandTimeout: commandTimeout,
	}
	s.regions = newRegionManager(cfg.Manifest, combinedHooks)
	s.command = newCommandPipeline(cfg.CommandApplier)
	s.operations = newOperationScheduler(queueSize, operationInterval, operationCooldown, operationBurst, s.applyOperation, s.operationHook)
	if cfg.Manifest != nil {
		s.regions.loadSnapshot(cfg.Manifest.RegionSnapshot())
	}
	if s.scheduler != nil {
		heartbeatInterval := cfg.HeartbeatInterval
		if heartbeatInterval <= 0 {
			heartbeatInterval = 3 * time.Second
		}
		// Heartbeat loop bridges local region/store state to the injected
		// scheduler sink and optionally drains scheduling operations.
		s.heartbeat = newHeartbeatLoop(
			heartbeatInterval,
			s.scheduler,
			planner,
			s.storeID,
			s.RegionMetas,
			s.storeStatsSnapshot,
			s.SchedulerSnapshot,
			s.enqueueOperation,
		)
		if s.heartbeat != nil {
			s.heartbeat.start()
		}
	}
	return s
}

func mergeRegionHooks(hooks ...RegionHooks) RegionHooks {
	update := func(meta manifest.RegionMeta) {
		for _, h := range hooks {
			if h.OnRegionUpdate != nil {
				h.OnRegionUpdate(meta)
			}
		}
	}
	remove := func(id uint64) {
		for _, h := range hooks {
			if h.OnRegionRemove != nil {
				h.OnRegionRemove(id)
			}
		}
	}
	return RegionHooks{
		OnRegionUpdate: func(meta manifest.RegionMeta) {
			if len(hooks) == 0 {
				return
			}
			update(meta)
		},
		OnRegionRemove: func(id uint64) {
			if len(hooks) == 0 {
				return
			}
			remove(id)
		},
	}
}
