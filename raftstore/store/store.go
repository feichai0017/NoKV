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
	router      *Router
	peerBuilder PeerBuilder
	workDir     string
	storeID     uint64
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

	mu            sync.Mutex
	pending       map[operationKey]struct{}
	lastApply     map[operationKey]time.Time
	dropped       uint64
	degraded      bool
	lastError     string
	lastErrorAt   time.Time
	heartbeat     time.Duration
	heartbeatStop chan struct{}
	heartbeatWG   sync.WaitGroup
}

type commandRuntime struct {
	apply   func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)
	pipe    *commandPipeline
	timeout time.Duration
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
		router:      router,
		peerBuilder: cfg.PeerBuilder,
		workDir:     cfg.WorkDir,
		storeID:     cfg.StoreID,
		regions: &regionRuntime{
			metrics: regionMetrics,
			mgr:     newRegionManager(cfg.LocalMeta, regionMetrics, cfg.Scheduler),
		},
		sched: &schedulerRuntime{
			client:    cfg.Scheduler,
			cooldown:  operationCooldown,
			interval:  operationInterval,
			burst:     operationBurst,
			pending:   make(map[operationKey]struct{}),
			lastApply: make(map[operationKey]time.Time),
		},
		cmds: &commandRuntime{
			apply:   cfg.CommandApplier,
			pipe:    newCommandPipeline(cfg.CommandApplier),
			timeout: commandTimeout,
		},
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
		s.regionMgr().loadSnapshot(cfg.LocalMeta.Snapshot())
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

// SchedulerStatus returns the current scheduler health view by combining the
// local queue state with the control-plane client status.
func (s *Store) SchedulerStatus() SchedulerStatus {
	if s == nil {
		return SchedulerStatus{}
	}
	status := SchedulerStatus{}
	if s.schedulerClient() != nil {
		status = s.schedulerClient().Status()
	}
	if s.sched == nil {
		return status
	}
	s.sched.mu.Lock()
	defer s.sched.mu.Unlock()
	status.DroppedOperations += s.sched.dropped
	if s.sched.degraded {
		status.Degraded = true
		if status.LastErrorAt.Before(s.sched.lastErrorAt) || status.LastError == "" {
			status.LastError = s.sched.lastError
			status.LastErrorAt = s.sched.lastErrorAt
		}
	}
	return status
}

func (s *Store) regionMgr() *regionManager {
	if s == nil || s.regions == nil {
		return nil
	}
	return s.regions.mgr
}

func (s *Store) regionMetrics() *metrics.RegionMetrics {
	if s == nil || s.regions == nil {
		return nil
	}
	return s.regions.metrics
}

func (s *Store) schedulerClient() SchedulerClient {
	if s == nil || s.sched == nil {
		return nil
	}
	return s.sched.client
}

func (s *Store) commandPipe() *commandPipeline {
	if s == nil || s.cmds == nil {
		return nil
	}
	return s.cmds.pipe
}

func (s *Store) commandApply() func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	if s == nil || s.cmds == nil {
		return nil
	}
	return s.cmds.apply
}

func (s *Store) commandWait() time.Duration {
	if s == nil || s.cmds == nil {
		return 0
	}
	return s.cmds.timeout
}
