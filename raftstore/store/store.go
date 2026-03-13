package store

import (
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// Store hosts a collection of peers and exposes the concrete runtime helpers
// used by raftstore. It owns peer registration, region metadata, optional
// control-plane heartbeats, and command application.
type Store struct {
	router         *Router
	peerBuilder    PeerBuilder
	regionMetrics  *metrics.RegionMetrics
	regions        *regionManager
	scheduler      SchedulerClient
	workDir        string
	storeID        uint64
	commandApplier func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)
	command        *commandPipeline
	commandTimeout time.Duration

	operationInput     chan Operation
	operationStop      chan struct{}
	operationWG        sync.WaitGroup
	operationCooldown  time.Duration
	operationInterval  time.Duration
	operationBurst     int
	operationMu        sync.Mutex
	operationPending   map[operationKey]struct{}
	operationLastApply map[operationKey]time.Time
	schedulerDropped   uint64
	schedulerDegraded  bool
	schedulerLastError string
	schedulerLastAt    time.Time

	heartbeatInterval time.Duration
	heartbeatStop     chan struct{}
	heartbeatWG       sync.WaitGroup
}

type operationKey struct {
	region uint64
	typeID OperationType
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
		router:             router,
		peerBuilder:        cfg.PeerBuilder,
		regionMetrics:      regionMetrics,
		scheduler:          cfg.Scheduler,
		workDir:            cfg.WorkDir,
		storeID:            cfg.StoreID,
		commandApplier:     cfg.CommandApplier,
		commandTimeout:     commandTimeout,
		operationCooldown:  operationCooldown,
		operationInterval:  operationInterval,
		operationBurst:     operationBurst,
		operationPending:   make(map[operationKey]struct{}),
		operationLastApply: make(map[operationKey]time.Time),
	}
	if s.workDir == "" && cfg.LocalMeta != nil {
		s.workDir = cfg.LocalMeta.WorkDir()
	}
	s.regions = newRegionManager(cfg.LocalMeta, regionMetrics, cfg.Scheduler)
	s.command = newCommandPipeline(cfg.CommandApplier)
	if queueSize > 0 {
		s.operationInput = make(chan Operation, queueSize)
		s.operationStop = make(chan struct{})
		s.operationWG.Add(1)
		go s.runOperationLoop()
	}
	if cfg.LocalMeta != nil {
		s.regions.loadSnapshot(cfg.LocalMeta.Snapshot())
	}
	if s.scheduler != nil {
		s.heartbeatInterval = cfg.HeartbeatInterval
		if s.heartbeatInterval <= 0 {
			s.heartbeatInterval = 3 * time.Second
		}
		s.startHeartbeatLoop()
	}
	return s
}

// SchedulerStatus returns the current scheduler health view by combining the
// local queue state with the control-plane client status.
func (s *Store) SchedulerStatus() SchedulerStatus {
	if s == nil {
		return SchedulerStatus{}
	}
	status := SchedulerStatus{}
	if s.scheduler != nil {
		status = s.scheduler.Status()
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	status.DroppedOperations += s.schedulerDropped
	if s.schedulerDegraded {
		status.Degraded = true
		if status.LastErrorAt.Before(s.schedulerLastAt) || status.LastError == "" {
			status.LastError = s.schedulerLastError
			status.LastErrorAt = s.schedulerLastAt
		}
	}
	return status
}
