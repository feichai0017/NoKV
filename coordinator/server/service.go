// Package server implements the Coordinator gRPC service — the control-plane
// entry point for route lookup, TSO, ID allocation, lease management, and
// rooted topology mutations.
//
// This package owns the SERVICE layer. It consumes rooted truth from
// meta/root/ but never owns durable cluster state. The execution plane
// (raftstore/) applies and publishes, and coordinator reconstructs its
// view by tailing rooted commits. Contracts between the planes are
// specified in TLA+ under spec/Succession.tla.
//
// Heavy logic is deliberately split into sibling packages:
// catalog (region/event validation), view (directory + store health),
// protocol/succession (authority handoff primitives), storage
// (rooted adapter), audit (snapshot + trace audit).
//
// Design references: docs/coordinator.md, docs/control_and_execution_protocols.md,
// docs/rooted_truth.md, docs/succession-audit.md.
package server

import (
	"sync"
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

	cluster           *catalog.Cluster
	ids               *idalloc.IDAllocator
	tso               *tso.Allocator
	storage           rootview.RootStorage
	idWindowHigh      uint64
	idWindowSize      uint64
	tsoWindowHigh     uint64
	tsoWindowSize     uint64
	allocMu           sync.Mutex
	writeMu           sync.Mutex
	leaseMu           sync.RWMutex
	coordinatorID     string
	leaseTTL          time.Duration
	leaseRenewIn      time.Duration
	leaseClockSkew    time.Duration
	now               func() time.Time
	leaseView         coordinatorLeaseView
	rootViewMu        sync.RWMutex
	rootView          coordinatorRootSnapshotView
	rootViewTTL       time.Duration
	statusMu          sync.RWMutex
	lastRootReload    int64
	lastRootError     string
	successionMetrics successionMetrics
	ablation          coordablation.Config
}

type coordinatorLeaseView struct {
	lease   rootstate.Tenure
	seal    rootstate.Legacy
	closure rootstate.Transit
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
	v.lease = rootstate.Tenure{}
	v.seal = rootstate.Legacy{}
	v.closure = rootstate.Transit{}
}

func (v *coordinatorLeaseView) Refresh(snapshot rootview.Snapshot) {
	if v == nil {
		return
	}
	v.lease = snapshot.Tenure
	v.seal = snapshot.Legacy
	v.closure = snapshot.Transit
}

func (v coordinatorLeaseView) Current() (rootstate.Tenure, rootstate.Legacy) {
	return v.lease, v.seal
}

func (v coordinatorLeaseView) Lease() rootstate.Tenure {
	return v.lease
}

func (v coordinatorLeaseView) Seal() rootstate.Legacy {
	return v.seal
}

func (v coordinatorLeaseView) Closure() rootstate.Transit {
	return v.closure
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
	return &Service{
		cluster:       cluster,
		ids:           ids,
		tso:           tsAlloc,
		storage:       storage,
		idWindowSize:  defaultAllocatorWindowSize,
		tsoWindowSize: defaultAllocatorWindowSize,
		now:           time.Now,
	}
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
