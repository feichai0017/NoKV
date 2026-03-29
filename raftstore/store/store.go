package store

import (
	"context"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// Store hosts a collection of peers and exposes the concrete runtime helpers
// used by raftstore. It owns peer registration, region metadata, optional
// control-plane heartbeats, and command application.
type Store struct {
	router      *Router
	peerBuilder PeerBuilder
	workDir     string
	storeID     uint64
	ctx         context.Context
	cancel      context.CancelFunc
	regions     *regionRuntime
	sched       *schedulerRuntime
	cmds        *commandRuntime
}

type operationKey struct {
	region uint64
	typeID OperationType
}

type regionRuntime struct {
	metrics *metrics.RegionMetrics
	mgr     *regionManager
}

type schedulerRuntime struct {
	client SchedulerClient

	input    chan Operation
	stop     chan struct{}
	wg       sync.WaitGroup
	cooldown time.Duration
	interval time.Duration
	burst    int

	regionSignal chan struct{}

	mu            sync.Mutex
	pending       map[operationKey]struct{}
	lastApply     map[operationKey]time.Time
	regionUpdates map[uint64]regionEvent
	dropped       uint64
	degraded      bool
	lastError     string
	lastErrorAt   time.Time
	heartbeat     time.Duration
	heartbeatStop chan struct{}
	heartbeatWG   sync.WaitGroup
}

type regionEventKind uint8

const (
	regionEventNone regionEventKind = iota
	regionEventApply
	regionEventRemove
)

type regionEvent struct {
	kind     regionEventKind
	regionID uint64
	meta     raftmeta.RegionMeta
}

// PeerHandle is a lightweight view of a peer registered with the store. It is
// designed for diagnostics and scheduling components so they can iterate over
// the cluster topology without touching the internal map directly.
type PeerHandle struct {
	ID     uint64
	Peer   *peer.Peer
	Region *raftmeta.RegionMeta
}

// RegionSnapshot provides an external view of the tracked Region metadata.
type RegionSnapshot struct {
	Regions []raftmeta.RegionMeta `json:"regions"`
}

// NewStore creates a Store with the provided router. When router is nil a new
// instance is allocated implicitly so callers can skip the explicit
// construction in tests.
func NewStore(router *Router) *Store {
	return NewStoreWithConfig(Config{Router: router})
}

// NewStoreWithConfig constructs a Store using concrete dependencies. It keeps
// peer construction, region tracking, and scheduler heartbeats explicit rather
// than routing them through callback chains.
func NewStoreWithConfig(cfg Config) *Store {
	ctx, cancel := context.WithCancel(context.Background())
	router := cfg.Router
	if router == nil {
		router = NewRouter()
	}
	regionMetrics := metrics.NewRegionMetrics()
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
		router:      router,
		peerBuilder: cfg.PeerBuilder,
		workDir:     cfg.WorkDir,
		storeID:     cfg.StoreID,
		ctx:         ctx,
		cancel:      cancel,
		sched: &schedulerRuntime{
			client:        cfg.Scheduler,
			cooldown:      operationCooldown,
			interval:      operationInterval,
			burst:         operationBurst,
			pending:       make(map[operationKey]struct{}),
			lastApply:     make(map[operationKey]time.Time),
			regionUpdates: make(map[uint64]regionEvent),
		},
		cmds: &commandRuntime{
			apply:   cfg.CommandApplier,
			pipe:    newCommandPipeline(cfg.CommandApplier),
			timeout: commandTimeout,
		},
	}
	s.regions = &regionRuntime{
		metrics: regionMetrics,
		mgr:     newRegionManager(cfg.LocalMeta, regionMetrics, s.enqueueRegionEvent),
	}
	if s.workDir == "" && cfg.LocalMeta != nil {
		s.workDir = cfg.LocalMeta.WorkDir()
	}
	if queueSize > 0 {
		s.sched.input = make(chan Operation, queueSize)
		s.sched.stop = make(chan struct{})
		s.sched.wg.Add(1)
		go s.runOperationLoop()
	}
	if cfg.LocalMeta != nil {
		s.regionMgr().loadBootstrapSnapshot(cfg.LocalMeta.Snapshot())
	}
	if s.schedulerClient() != nil {
		s.sched.heartbeat = cfg.HeartbeatInterval
		if s.sched.heartbeat <= 0 {
			s.sched.heartbeat = 3 * time.Second
		}
		s.startHeartbeatLoop()
	}
	return s
}
