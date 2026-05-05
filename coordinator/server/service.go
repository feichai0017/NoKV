// Package server implements the Coordinator gRPC service — the control-plane
// entry point for route lookup, TSO, ID allocation, grant management, and
// rooted topology mutations.
//
// This package owns the SERVICE layer. It consumes rooted truth from
// meta/root/ but never owns durable cluster state. The execution plane
// (raftstore/) applies and publishes, and coordinator reconstructs its
// view by tailing rooted commits.
//
// Heavy logic is deliberately split into sibling packages:
// catalog (region/event validation), view (directory + store health),
// protocol/eunomia (authority handoff primitives), storage
// (rooted adapter), audit (snapshot + trace audit).
//
// Design references: docs/coordinator.md, docs/control_and_execution_protocols.md,
// docs/rooted_truth.md.
package server

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// Service implements the Coordinator gRPC API.
//
// Lock order:
//  1. writeMu
//  2. allocMu
//  3. grantMu
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
	grantMu           sync.RWMutex
	coordinatorID     string
	grantTTL          time.Duration
	grantRenewIn      time.Duration
	grantClockSkew    time.Duration
	now               func() time.Time
	grantView         coordinatorGrantView
	authorityMu       sync.Mutex
	authorityState    authorityServingState
	authorityInflight uint64
	rootViewMu        sync.RWMutex
	rootView          coordinatorRootSnapshotView
	rootViewTTL       time.Duration
	// storeHeartbeatTTL holds the time.Duration value as an int64 so callers
	// (storeState reads, ConfigureStoreHeartbeatTTL writes) avoid a data race
	// without taking a lock on the read path.
	storeHeartbeatTTL atomic.Int64
	statusMu          sync.RWMutex
	lastRootReload    int64
	lastRootError     string
	eunomiaMetrics    eunomiaMetrics
}

type authorityServingState uint8

const (
	authorityServing authorityServingState = iota
	authorityDraining
	authoritySealed
)

func (s authorityServingState) String() string {
	switch s {
	case authorityServing:
		return "serving"
	case authorityDraining:
		return "draining"
	case authoritySealed:
		return "sealed"
	default:
		return "unknown"
	}
}

type coordinatorGrantView struct {
	grant        rootproto.AuthorityGrant
	retirements  []rootproto.GrantRetirement
	inheritances []rootproto.GrantInheritance
}

type coordinatorRootSnapshotView struct {
	snapshot    rootview.Snapshot
	loaded      bool
	refreshing  bool
	refreshedAt time.Time
}

func (v *coordinatorGrantView) Reset() {
	if v == nil {
		return
	}
	v.grant = rootproto.AuthorityGrant{}
	v.retirements = nil
	v.inheritances = nil
}

func (v *coordinatorGrantView) Refresh(snapshot rootview.Snapshot) {
	if v == nil {
		return
	}
	v.grant = snapshot.ActiveGrant
	v.retirements = append([]rootproto.GrantRetirement(nil), snapshot.RetiredGrants...)
	v.inheritances = append([]rootproto.GrantInheritance(nil), snapshot.GrantInheritances...)
}

func (v coordinatorGrantView) Grant() rootproto.AuthorityGrant {
	return v.grant
}

func (v coordinatorGrantView) Retirements() []rootproto.GrantRetirement {
	return append([]rootproto.GrantRetirement(nil), v.retirements...)
}

const defaultAllocatorWindowSize uint64 = 10_000
const defaultGrantTTL = 10 * time.Second
const defaultGrantRenewIn = 3 * time.Second
const defaultGrantClockSkew = 500 * time.Millisecond
const defaultGrantRetryMin = 200 * time.Millisecond
const maxGrantRetry = 60 * time.Second
const defaultGrantReleaseTimeout = 2 * time.Second
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

// ConfigureAuthorityGrant enables the explicit coordinator owner grant gate.
// Empty holderID disables the gate and keeps the current in-memory-only behavior.
func (s *Service) ConfigureAuthorityGrant(holderID string, ttl, renewIn time.Duration) {
	if s == nil {
		return
	}
	s.grantMu.Lock()
	defer s.grantMu.Unlock()
	s.coordinatorID = holderID
	s.resetAuthorityServing()
	if holderID == "" {
		s.grantTTL = 0
		s.grantRenewIn = 0
		s.grantClockSkew = 0
		s.grantView.Reset()
		return
	}
	if ttl <= 0 {
		ttl = defaultGrantTTL
	}
	if renewIn <= 0 || renewIn >= ttl {
		renewIn = defaultGrantRenewIn
		if renewIn >= ttl {
			renewIn = ttl / 2
		}
	}
	clockSkew := defaultGrantClockSkew
	if clockSkew >= renewIn && renewIn > 0 {
		clockSkew = renewIn / 2
	}
	if clockSkew <= 0 {
		clockSkew = time.Millisecond
	}
	s.grantTTL = ttl
	s.grantRenewIn = renewIn
	s.grantClockSkew = clockSkew
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
