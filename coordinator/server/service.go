// Package server implements the Coordinator gRPC service — the control-plane
// entry point for route lookup, TSO, ID allocation, lease management, and
// rooted topology mutations.
//
// This package owns the SERVICE layer. It consumes rooted truth from
// meta/root/ but never owns durable cluster state. The execution plane
// (raftstore/) applies and publishes, and coordinator reconstructs its
// view by tailing rooted commits. Contracts between the planes are
// specified in TLA+ under docs/spec/Eunomia.tla.
//
// Heavy logic is deliberately split into sibling packages:
// catalog (region/event validation), view (directory + store health),
// protocol/eunomia (authority handoff primitives), storage
// (rooted adapter), audit (snapshot + trace audit).
//
// Design references: docs/coordinator.md, docs/control_and_execution_protocols.md,
// docs/rooted_truth.md, docs/eunomia-audit.md.
package server

import (
	"sync"
	"sync/atomic"
	"time"

	coordablation "github.com/feichai0017/NoKV/coordinator/ablation"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// Service implements the Coordinator gRPC API.
//
// Lock order:
//  1. writeMu
//  2. allocMu
//  3. leaseMu
//
// Never acquire these locks in reverse order.
type Service struct {
	coordpb.UnimplementedCoordinatorServer

	cluster        *catalog.Cluster
	ids            *idalloc.IDAllocator
	tso            *tso.Allocator
	storage        rootview.RootStorage
	idWindowHigh   uint64
	idWindowSize   uint64
	tsoWindowHigh  uint64
	tsoWindowSize  uint64
	allocMu        sync.Mutex
	writeMu        sync.Mutex
	leaseMu        sync.RWMutex
	coordinatorID  string
	leaseTTL       time.Duration
	leaseRenewIn   time.Duration
	leaseClockSkew time.Duration
	now            func() time.Time
	leaseView      coordinatorLeaseView
	rootViewMu     sync.RWMutex
	rootView       coordinatorRootSnapshotView
	rootViewTTL    time.Duration
	// storeHeartbeatTTL holds the time.Duration value as an int64 so callers
	// (storeState reads, ConfigureStoreHeartbeatTTL writes) avoid a data race
	// without taking a lock on the read path.
	storeHeartbeatTTL atomic.Int64
	statusMu          sync.RWMutex
	lastRootReload    int64
	lastRootError     string
	eunomiaMetrics    eunomiaMetrics
	ablation          coordablation.Config
}

type coordinatorLeaseView struct {
	tenure   rootstate.Tenure
	legacy   rootstate.Legacy
	handover rootstate.Handover
}

type coordinatorRootSnapshotView struct {
	snapshot    rootview.Snapshot
	loaded      bool
	refreshing  bool
	refreshedAt time.Time
}

func (v *coordinatorLeaseView) Reset() {
	if v == nil {
		return
	}
	v.tenure = rootstate.Tenure{}
	v.legacy = rootstate.Legacy{}
	v.handover = rootstate.Handover{}
}

func (v *coordinatorLeaseView) Refresh(snapshot rootview.Snapshot) {
	if v == nil {
		return
	}
	v.tenure = snapshot.Tenure
	v.legacy = snapshot.Legacy
	v.handover = snapshot.Handover
}

func (v coordinatorLeaseView) Current() (rootstate.Tenure, rootstate.Legacy) {
	return v.tenure, v.legacy
}

func (v coordinatorLeaseView) Tenure() rootstate.Tenure {
	return v.tenure
}

func (v coordinatorLeaseView) Legacy() rootstate.Legacy {
	return v.legacy
}

func (v coordinatorLeaseView) Handover() rootstate.Handover {
	return v.handover
}

const defaultAllocatorWindowSize uint64 = 10_000
const ablationUnlimitedWindowSize uint64 = 1 << 20
const defaultTenureTTL = 10 * time.Second
const defaultTenureRenewIn = 3 * time.Second
const defaultTenureClockSkew = 500 * time.Millisecond
const defaultTenureRetryMin = 200 * time.Millisecond
const maxTenureRetry = 60 * time.Second
const defaultTenureReleaseTimeout = 2 * time.Second
const defaultRootSnapshotRefreshInterval = 250 * time.Millisecond
const defaultStoreHeartbeatTTL = 10 * time.Second

// NewService constructs a Coordinator service. The optional root storage fixes
// durable rooted persistence at construction time; omitting it keeps the service
// in explicit in-memory mode.
func NewService(cluster *catalog.Cluster, ids *idalloc.IDAllocator, tsAlloc *tso.Allocator, root ...rootview.RootStorage) *Service {
	if cluster == nil {
		cluster = catalog.NewCluster()
	}
	if ids == nil {
		ids = idalloc.NewIDAllocator(1)
	}
	if tsAlloc == nil {
		tsAlloc = tso.NewAllocator(1)
	}
	var storage rootview.RootStorage
	if len(root) > 0 {
		storage = root[0]
	}
	svc := &Service{
		cluster:       cluster,
		ids:           ids,
		tso:           tsAlloc,
		storage:       storage,
		idWindowSize:  defaultAllocatorWindowSize,
		tsoWindowSize: defaultAllocatorWindowSize,
		now:           time.Now,
	}
	svc.storeHeartbeatTTL.Store(int64(defaultStoreHeartbeatTTL))
	return svc
}

// ConfigureTenure enables the explicit coordinator owner lease gate.
// Empty holderID disables the gate and keeps the current in-memory-only behavior.
func (s *Service) ConfigureTenure(holderID string, ttl, renewIn time.Duration) {
	if s == nil {
		return
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	s.coordinatorID = holderID
	if holderID == "" {
		s.leaseTTL = 0
		s.leaseRenewIn = 0
		s.leaseClockSkew = 0
		s.leaseView.Reset()
		return
	}
	if ttl <= 0 {
		ttl = defaultTenureTTL
	}
	if renewIn <= 0 || renewIn >= ttl {
		renewIn = defaultTenureRenewIn
		if renewIn >= ttl {
			renewIn = ttl / 2
		}
	}
	clockSkew := defaultTenureClockSkew
	if clockSkew >= renewIn && renewIn > 0 {
		clockSkew = renewIn / 2
	}
	if clockSkew <= 0 {
		clockSkew = time.Millisecond
	}
	s.leaseTTL = ttl
	s.leaseRenewIn = renewIn
	s.leaseClockSkew = clockSkew
}

// ConfigureAllocatorWindows overrides the rooted allocator refill window sizes.
// Zero values keep the default window behavior.
func (s *Service) ConfigureAllocatorWindows(idWindowSize, tsoWindowSize uint64) {
	if s == nil {
		return
	}
	if idWindowSize != 0 {
		s.idWindowSize = idWindowSize
	}
	if tsoWindowSize != 0 {
		s.tsoWindowSize = tsoWindowSize
	}
}

// ConfigureStoreHeartbeatTTL controls when the runtime store registry marks a
// store as down after its last heartbeat. Non-positive values keep the default.
// Safe to call concurrently with RPC handlers that read storeHeartbeatTTL via
// atomic load (see storeState in service_gateway.go).
func (s *Service) ConfigureStoreHeartbeatTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = defaultStoreHeartbeatTTL
	}
	s.storeHeartbeatTTL.Store(int64(ttl))
}

// ConfigureAblation installs first-cut experimental switches used by the
// control-plane ablation runner.
func (s *Service) ConfigureAblation(cfg coordablation.Config) error {
	if s == nil {
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	s.ablation = cfg
	return nil
}

// ConfigureRootSnapshotRefresh controls how long GetRegionByKey keeps one
// cached rooted snapshot before refreshing it asynchronously.
func (s *Service) ConfigureRootSnapshotRefresh(interval time.Duration) {
	if s == nil {
		return
	}
	s.rootViewTTL = interval
}

// RefreshFromStorage refreshes rooted durable state into the in-memory service
// view and fences allocator state so a future leader cannot allocate stale ids.
func (s *Service) RefreshFromStorage() error {
	if s == nil || s.storage == nil {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.reloadAndFenceAllocators(true)
}

// ReloadFromStorage reloads the in-memory rooted view from the storage cache
// without forcing the underlying rooted backend to refresh first.
func (s *Service) ReloadFromStorage() error {
	if s == nil || s.storage == nil {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.reloadAndFenceAllocators(false)
}
