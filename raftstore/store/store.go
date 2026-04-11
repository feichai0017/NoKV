package store

import (
	"context"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"time"
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
	exec        *executionRuntime
}

// NewStore constructs a Store using concrete dependencies. It keeps peer
// construction, region tracking, and scheduler heartbeats explicit rather than
// routing them through callback chains.
func NewStore(cfg Config) *Store {
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
	heartbeatTimeout := cfg.HeartbeatTimeout
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 2 * time.Second
	}
	publishTimeout := cfg.PublishTimeout
	if publishTimeout <= 0 {
		publishTimeout = 2 * time.Second
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
			client: cfg.Scheduler,
			operation: operationRuntime{
				cooldown:  operationCooldown,
				interval:  operationInterval,
				burst:     operationBurst,
				pending:   make(map[operationKey]struct{}),
				lastApply: make(map[operationKey]time.Time),
			},
			publish: publishRuntime{
				descriptors:      make(map[uint64]descriptor.Descriptor),
				regionUpdates:    make(map[uint64]regionEvent),
				heartbeatTimeout: heartbeatTimeout,
				publishTimeout:   publishTimeout,
			},
		},
		cmds: &commandRuntime{
			apply:   cfg.CommandApplier,
			pipe:    newCommandPipeline(cfg.CommandApplier),
			timeout: commandTimeout,
		},
		exec: newExecutionRuntime(),
	}
	s.regions = &regionRuntime{
		metrics: regionMetrics,
		mgr:     newRegionManager(cfg.LocalMeta, regionMetrics, s.enqueueRegionEvent),
	}
	if s.workDir == "" && cfg.LocalMeta != nil {
		s.workDir = cfg.LocalMeta.WorkDir()
	}
	if queueSize > 0 {
		s.sched.operation.input = make(chan Operation, queueSize)
		s.sched.operation.stop = make(chan struct{})
		s.sched.operation.wg.Add(1)
		go s.runOperationLoop()
	}
	if cfg.LocalMeta != nil {
		s.regionMgr().loadBootstrapSnapshot(cfg.LocalMeta.Snapshot())
		s.enqueueRecoveredPendingRegionEvents(cfg.LocalMeta.PendingRootEvents())
	}
	if s.schedulerClient() != nil {
		s.sched.publish.heartbeat = cfg.HeartbeatInterval
		if s.sched.publish.heartbeat <= 0 {
			s.sched.publish.heartbeat = 3 * time.Second
		}
		s.startHeartbeatLoop()
		s.signalRegionFlush()
	}
	return s
}

// WorkDir returns the store-local workdir used for metadata and staging.
func (s *Store) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.workDir
}

func (s *Store) runtimeContext() context.Context {
	if s == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *Store) schedulerHeartbeatContext() (context.Context, context.CancelFunc) {
	if s == nil || s.sched == nil || s.sched.publish.heartbeatTimeout <= 0 {
		return context.WithCancel(s.runtimeContext())
	}
	return context.WithTimeout(s.runtimeContext(), s.sched.publish.heartbeatTimeout)
}

func (s *Store) schedulerPublishContext() (context.Context, context.CancelFunc) {
	if s == nil || s.sched == nil || s.sched.publish.publishTimeout <= 0 {
		return context.WithCancel(s.runtimeContext())
	}
	return context.WithTimeout(s.runtimeContext(), s.sched.publish.publishTimeout)
}
