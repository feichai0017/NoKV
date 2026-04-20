package server

import (
	"context"
	"errors"
	"fmt"
	coordablation "github.com/feichai0017/NoKV/coordinator/ablation"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordprotocol "github.com/feichai0017/NoKV/coordinator/protocol"
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/coordinator/tso"
	pdview "github.com/feichai0017/NoKV/coordinator/view"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
	"time"
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
	storage        coordstorage.RootStorage
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
	statusMu       sync.RWMutex
	lastRootReload int64
	lastRootError  string
	ablation       coordablation.Config
}

type coordinatorLeaseView struct {
	lease rootstate.CoordinatorLease
	seal  rootstate.CoordinatorSeal
}

func (v *coordinatorLeaseView) Reset() {
	if v == nil {
		return
	}
	v.lease = rootstate.CoordinatorLease{}
	v.seal = rootstate.CoordinatorSeal{}
}

func (v *coordinatorLeaseView) Refresh(snapshot coordstorage.Snapshot) {
	if v == nil {
		return
	}
	v.lease = snapshot.CoordinatorLease
	v.seal = snapshot.CoordinatorSeal
}

func (v coordinatorLeaseView) Current() (rootstate.CoordinatorLease, rootstate.CoordinatorSeal) {
	return v.lease, v.seal
}

func (v coordinatorLeaseView) Lease() rootstate.CoordinatorLease {
	return v.lease
}

func (v coordinatorLeaseView) Seal() rootstate.CoordinatorSeal {
	return v.seal
}

const defaultAllocatorWindowSize uint64 = 10_000
const ablationUnlimitedWindowSize uint64 = 1 << 20
const defaultCoordinatorLeaseTTL = 10 * time.Second
const defaultCoordinatorLeaseRenewIn = 3 * time.Second
const defaultCoordinatorLeaseClockSkew = 500 * time.Millisecond

// NewService constructs a Coordinator service. The optional root storage fixes
// durable rooted persistence at construction time; omitting it keeps the service
// in explicit in-memory mode.
func NewService(cluster *catalog.Cluster, ids *idalloc.IDAllocator, tsAlloc *tso.Allocator, root ...coordstorage.RootStorage) *Service {
	if cluster == nil {
		cluster = catalog.NewCluster()
	}
	if ids == nil {
		ids = idalloc.NewIDAllocator(1)
	}
	if tsAlloc == nil {
		tsAlloc = tso.NewAllocator(1)
	}
	var storage coordstorage.RootStorage
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

// ConfigureCoordinatorLease enables the explicit coordinator owner lease gate.
// Empty holderID disables the gate and keeps the current in-memory-only behavior.
func (s *Service) ConfigureCoordinatorLease(holderID string, ttl, renewIn time.Duration) {
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
		ttl = defaultCoordinatorLeaseTTL
	}
	if renewIn <= 0 || renewIn >= ttl {
		renewIn = defaultCoordinatorLeaseRenewIn
		if renewIn >= ttl {
			renewIn = ttl / 2
		}
	}
	clockSkew := defaultCoordinatorLeaseClockSkew
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

// StoreHeartbeat records store-level stats.
func (s *Service) StoreHeartbeat(_ context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "store heartbeat request is nil")
	}
	err := s.cluster.UpsertStoreHeartbeat(pdview.StoreStats{
		StoreID:   req.GetStoreId(),
		RegionNum: req.GetRegionNum(),
		LeaderNum: req.GetLeaderNum(),
		Capacity:  req.GetCapacity(),
		Available: req.GetAvailable(),
	})
	if err != nil {
		if errors.Is(err, catalog.ErrInvalidStoreID) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	operations := s.leaseScopedStoreOperations(req.GetStoreId())
	return &coordpb.StoreHeartbeatResponse{
		Accepted:   true,
		Operations: operations,
	}, nil
}

// RegionLiveness records one runtime heartbeat without mutating rooted truth.
func (s *Service) RegionLiveness(_ context.Context, req *coordpb.RegionLivenessRequest) (*coordpb.RegionLivenessResponse, error) {
	if req == nil || req.GetRegionId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region liveness request missing region_id")
	}
	accepted := s.cluster.TouchRegionHeartbeat(req.GetRegionId())
	return &coordpb.RegionLivenessResponse{Accepted: accepted}, nil
}

// PublishRootEvent records one explicit rooted topology truth event.
func (s *Service) PublishRootEvent(_ context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	if req == nil || req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "publish root event request missing event")
	}
	event := metawire.RootEventFromProto(req.GetEvent())
	if event.Kind == rootevent.KindUnknown {
		return nil, status.Error(codes.InvalidArgument, "publish root event requires known kind")
	}
	event, err := s.normalizeRootEvent(event)
	if err != nil {
		return nil, status.Error(codes.Internal, "normalize root event: "+err.Error())
	}
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.requireExpectedClusterEpoch(req.GetExpectedClusterEpoch()); err != nil {
		return nil, err
	}
	assessment, err := s.assessRootEventLifecycle(event)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	resp := &coordpb.PublishRootEventResponse{
		Assessment: transitionAssessmentToProto(assessment),
	}
	if assessment.Decision == rootstate.RootEventLifecycleSkip {
		resp.Accepted = true
		return resp, nil
	}
	if err := s.cluster.ValidateRootEvent(event); err != nil {
		switch {
		case errors.Is(err, catalog.ErrInvalidRegionID):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, catalog.ErrRegionHeartbeatStale), errors.Is(err, catalog.ErrRegionRangeOverlap):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if s.storage != nil {
		if err := s.storage.AppendRootEvent(event); err != nil {
			return nil, status.Error(codes.Internal, "persist root event: "+err.Error())
		}
		if _, err := s.reloadRootedView(false); err != nil {
			return nil, status.Error(codes.Internal, "reload rooted view: "+err.Error())
		}
		resp.Accepted = true
		return resp, nil
	}
	if err := s.cluster.PublishRootEvent(event); err != nil {
		return nil, status.Error(codes.Internal, "apply root event after persist: "+err.Error())
	}
	resp.Accepted = true
	return resp, nil
}

func (s *Service) assessRootEventLifecycle(event rootevent.Event) (rootstate.TransitionAssessment, error) {
	if s == nil || s.storage == nil {
		if s == nil || s.cluster == nil {
			return rootstate.TransitionAssessment{}, nil
		}
		return s.cluster.ObserveRootEventLifecycle(event), nil
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return rootstate.TransitionAssessment{}, fmt.Errorf("load rooted snapshot: %w", err)
	}
	rooted := rootstate.Snapshot{
		Descriptors:         snapshot.Descriptors,
		PendingPeerChanges:  snapshot.PendingPeerChanges,
		PendingRangeChanges: snapshot.PendingRangeChanges,
	}
	assessment := rootstate.AssessTransition(rooted, event)
	_, err = rootstate.EvaluateRootEventLifecycle(rooted, event)
	return assessment, err
}

// RemoveRegion deletes region metadata from the Coordinator in-memory catalog.
func (s *Service) RemoveRegion(_ context.Context, req *coordpb.RemoveRegionRequest) (*coordpb.RemoveRegionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "remove region request is nil")
	}
	regionID := req.GetRegionId()
	if regionID == 0 {
		return nil, status.Error(codes.InvalidArgument, "remove region requires region_id > 0")
	}
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	removed := s.cluster.HasRegion(regionID)
	if !removed {
		return &coordpb.RemoveRegionResponse{Removed: false}, nil
	}
	_, err := s.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metawire.RootEventToProto(rootevent.RegionTombstoned(regionID)),
		ExpectedClusterEpoch: req.GetExpectedClusterEpoch(),
	})
	if err != nil {
		return nil, err
	}
	return &coordpb.RemoveRegionResponse{Removed: true}, nil
}

func (s *Service) reloadRootedView(refresh bool) (coordstorage.Snapshot, error) {
	if s == nil || s.storage == nil {
		return coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
	}
	if refresh {
		if err := s.storage.Refresh(); err != nil {
			return coordstorage.Snapshot{}, err
		}
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return coordstorage.Snapshot{}, err
	}
	s.cluster.ReplaceRootSnapshot(snapshot.Descriptors, snapshot.PendingPeerChanges, snapshot.PendingRangeChanges, snapshot.RootToken)
	return snapshot, nil
}

func (s *Service) reloadAndFenceAllocators(refresh bool) error {
	snapshot, err := s.reloadRootedView(refresh)
	if err != nil {
		s.setLastRootReload(err)
		return err
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()
	s.fenceIDFromStorage(snapshot.Allocator.IDCurrent)
	s.fenceTSOFromStorage(snapshot.Allocator.TSCurrent)
	s.refreshLeaseMirror(snapshot)
	s.setLastRootReload(nil)
	return nil
}

func (s *Service) refreshLeaseMirror(snapshot coordstorage.Snapshot) {
	if s == nil {
		return
	}
	s.leaseMu.Lock()
	s.leaseView.Refresh(snapshot)
	s.leaseMu.Unlock()
}

// GetRegionByKey returns region metadata for the specified key.
func (s *Service) GetRegionByKey(_ context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get region by key request is nil")
	}
	state, err := s.currentReadState()
	admission, err := s.admitMetadataAnswerability(req, state, err)
	if err != nil {
		return nil, err
	}
	desc, ok := s.cluster.GetRegionDescriptorByKey(req.GetKey())
	if !ok {
		resp := admission.responseBase()
		resp.NotFound = true
		return resp, nil
	}
	if err := admission.admitDescriptorRevision(desc.RootEpoch); err != nil {
		return nil, err
	}
	resp := admission.responseBase()
	resp.RegionDescriptor = metawire.DescriptorToProto(desc)
	resp.DescriptorRevision = desc.RootEpoch
	return resp, nil
}

type readState struct {
	servedToken    rootstorage.TailToken
	currentToken   rootstorage.TailToken
	rootLag        uint64
	catchUpState   coordstorage.CatchUpState
	degraded       coordpb.DegradedMode
	servedByLeader bool
	certGeneration uint64
	sealGeneration uint64
	leasePresent   bool
	leaseActive    bool
	leaseSealed    bool
	leaseDutyMask  uint32
}

type metadataAnswerability struct {
	state                      readState
	freshness                  coordpb.Freshness
	requiredRootToken          rootstorage.TailToken
	requiredDescriptorRevision uint64
	maxRootLag                 *uint64
	servingClass               coordpb.ServingClass
	syncHealth                 coordpb.SyncHealth
}

func (a metadataAnswerability) admitDescriptorRevision(revision uint64) error {
	if revision < a.requiredDescriptorRevision {
		return status.Error(codes.FailedPrecondition, errRequiredDescriptorNotSatisfied)
	}
	return nil
}

func (a metadataAnswerability) responseBase() *coordpb.GetRegionByKeyResponse {
	return &coordpb.GetRegionByKeyResponse{
		NotFound:                   false,
		ServedRootToken:            rootTokenToProto(a.state.servedToken),
		ServedFreshness:            a.freshness,
		DegradedMode:               a.state.degraded,
		ServedByLeader:             a.state.servedByLeader,
		CurrentRootToken:           rootTokenToProto(a.state.currentToken),
		RootLag:                    a.state.rootLag,
		CatchUpState:               catchUpStateToProto(a.state.catchUpState),
		RequiredDescriptorRevision: a.requiredDescriptorRevision,
		CertGeneration:             a.state.certGeneration,
		ObservedSealGeneration:     a.state.sealGeneration,
		ServingClass:               a.servingClass,
		SyncHealth:                 a.syncHealth,
	}
}

func (s *Service) admitMetadataAnswerability(req *coordpb.GetRegionByKeyRequest, state readState, loadErr error) (metadataAnswerability, error) {
	admission := metadataAnswerability{
		state:                      state,
		freshness:                  coordprotocol.NormalizeFreshness(req.GetFreshness()),
		requiredRootToken:          rootTokenFromProto(req.GetRequiredRootToken()),
		requiredDescriptorRevision: req.GetRequiredDescriptorRevision(),
		maxRootLag:                 req.MaxRootLag,
	}
	if loadErr != nil {
		if s != nil && s.ablation.FailStopOnRootUnreach {
			return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRootUnavailable)
		}
		if admission.freshness == coordpb.Freshness_FRESHNESS_STRONG || admission.freshness == coordpb.Freshness_FRESHNESS_BOUNDED {
			return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRootUnavailable)
		}
		admission.state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE
	}
	if loadErr == nil && s != nil && s.coordinatorLeaseEnabled() && admission.state.leasePresent {
		if admission.state.leaseDutyMask&rootproto.CoordinatorDutyGetRegionByKey == 0 {
			return metadataAnswerability{}, statusCoordinatorLease(fmt.Errorf("%w: required_duty_mask=%d rooted_duty_mask=%d generation=%d", rootstate.ErrCoordinatorLeaseDuty, rootproto.CoordinatorDutyGetRegionByKey, admission.state.leaseDutyMask, admission.state.certGeneration))
		}
		if !admission.state.leaseActive {
			return metadataAnswerability{}, statusCoordinatorLease(fmt.Errorf("%w: rooted lease expired generation=%d", rootstate.ErrInvalidCoordinatorLease, admission.state.certGeneration))
		}
		if admission.state.leaseSealed {
			return metadataAnswerability{}, statusCoordinatorLease(fmt.Errorf("%w: generation=%d sealed_generation=%d", rootstate.ErrCoordinatorLeaseHeld, admission.state.certGeneration, admission.state.certGeneration))
		}
	}
	if !rootTokenSatisfied(admission.state.servedToken, admission.requiredRootToken) {
		return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRequiredRootedTokenNotSatisfied)
	}
	if admission.freshness == coordpb.Freshness_FRESHNESS_BOUNDED &&
		admission.maxRootLag != nil &&
		!boundedLagSatisfied(admission.state.rootLag, *admission.maxRootLag) {
		return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRootLagExceedsBound)
	}
	servingClass, syncHealth, err := s.admitReadServing(admission.freshness, admission.state)
	if err != nil {
		return metadataAnswerability{}, err
	}
	admission.servingClass = servingClass
	admission.syncHealth = syncHealth
	return admission, nil
}

func (s *Service) currentReadState() (readState, error) {
	if s == nil {
		return readState{
			degraded:       coordpb.DegradedMode_DEGRADED_MODE_HEALTHY,
			servedByLeader: true,
			catchUpState:   coordstorage.CatchUpStateFresh,
		}, nil
	}
	servedToken := rootstorage.TailToken{}
	if s.cluster != nil {
		servedToken = s.cluster.CatalogRootToken()
	}
	state := readState{
		degraded:       coordpb.DegradedMode_DEGRADED_MODE_HEALTHY,
		servedByLeader: s.storage == nil || s.storage.IsLeader(),
		servedToken:    servedToken,
		currentToken:   servedToken,
		catchUpState:   coordstorage.CatchUpStateFresh,
	}
	if s.storage == nil {
		state.rootLag = rootLag(state.currentToken, state.servedToken)
		return state, nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()
	snapshot, err := s.storage.Load()
	if err != nil {
		state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE
		state.catchUpState = coordstorage.CatchUpStateUnavailable
		return state, err
	}
	if snapshot.CoordinatorLease.CertGeneration != 0 && strings.TrimSpace(snapshot.CoordinatorLease.HolderID) != "" {
		state.leasePresent = true
		state.leaseActive = snapshot.CoordinatorLease.ActiveAt(nowUnixNano)
		state.leaseSealed = rootstate.CoordinatorGenerationSealed(snapshot.CoordinatorLease, snapshot.CoordinatorSeal)
		state.leaseDutyMask = snapshot.CoordinatorLease.DutyMask
	}
	if snapshot.CoordinatorSeal.Present() {
		state.sealGeneration = snapshot.CoordinatorSeal.CertGeneration
	}
	state.currentToken = snapshot.RootToken
	state.rootLag = rootLag(state.currentToken, state.servedToken)
	state.catchUpState = snapshot.CatchUpState
	state.certGeneration = s.metadataReplyGeneration(snapshot.CoordinatorLease.CertGeneration)
	if state.rootLag == 0 {
		state.catchUpState = coordstorage.CatchUpStateFresh
		return state, nil
	}
	if state.catchUpState == coordstorage.CatchUpStateFresh || state.catchUpState == coordstorage.CatchUpStateUnspecified {
		state.catchUpState = coordstorage.CatchUpStateLagging
	}
	if state.rootLag > 0 {
		state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING
	}
	return state, nil
}

func (s *Service) notLeaderError() error {
	if s == nil || s.storage == nil {
		return statusNotLeader(0)
	}
	leaderID := s.storage.LeaderID()
	if leaderID == 0 {
		return statusNotLeader(0)
	}
	return statusNotLeader(leaderID)
}

func rootTokenToProto(token rootstorage.TailToken) *coordpb.RootToken {
	return &coordpb.RootToken{
		Term:     token.Cursor.Term,
		Index:    token.Cursor.Index,
		Revision: token.Revision,
	}
}

func rootTokenFromProto(token *coordpb.RootToken) rootstorage.TailToken {
	if token == nil {
		return rootstorage.TailToken{}
	}
	return rootstorage.TailToken{
		Cursor: rootstate.Cursor{
			Term:  token.GetTerm(),
			Index: token.GetIndex(),
		},
		Revision: token.GetRevision(),
	}
}

func rootTokenSatisfied(current, required rootstorage.TailToken) bool {
	if required.Cursor.Term == 0 && required.Cursor.Index == 0 && required.Revision == 0 {
		return true
	}
	if current.Revision != 0 || required.Revision != 0 {
		return current.Revision >= required.Revision && !rootstate.CursorAfter(required.Cursor, current.Cursor)
	}
	return !rootstate.CursorAfter(required.Cursor, current.Cursor)
}

func rootLag(current, served rootstorage.TailToken) uint64 {
	if current.Revision > 0 || served.Revision > 0 {
		if current.Revision > served.Revision {
			return current.Revision - served.Revision
		}
		if current.Revision == served.Revision && rootstate.CursorAfter(current.Cursor, served.Cursor) {
			return 1
		}
		return 0
	}
	if rootstate.CursorAfter(current.Cursor, served.Cursor) {
		return 1
	}
	return 0
}

func boundedLagSatisfied(lag, bound uint64) bool {
	return lag <= bound
}

func catchUpStateToProto(state coordstorage.CatchUpState) coordpb.CatchUpState {
	switch state {
	case coordstorage.CatchUpStateFresh:
		return coordpb.CatchUpState_CATCH_UP_STATE_FRESH
	case coordstorage.CatchUpStateLagging:
		return coordpb.CatchUpState_CATCH_UP_STATE_LAGGING
	case coordstorage.CatchUpStateBootstrapRequired:
		return coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED
	case coordstorage.CatchUpStateUnavailable:
		return coordpb.CatchUpState_CATCH_UP_STATE_UNAVAILABLE
	default:
		return coordpb.CatchUpState_CATCH_UP_STATE_UNSPECIFIED
	}
}

func (s *Service) admitReadServing(freshness coordpb.Freshness, state readState) (coordpb.ServingClass, coordpb.SyncHealth, error) {
	servingClass, syncHealth := coordprotocol.MetadataServingContract(
		state.degraded,
		catchUpStateToProto(state.catchUpState),
		state.rootLag,
		state.servedByLeader,
	)

	switch freshness {
	case coordpb.Freshness_FRESHNESS_STRONG:
		if servingClass != coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE {
			if !state.servedByLeader {
				return servingClass, syncHealth, s.notLeaderError()
			}
			return servingClass, syncHealth, status.Error(codes.FailedPrecondition, errRootLagExceedsStrongFreshness)
		}
	case coordpb.Freshness_FRESHNESS_BOUNDED:
		if servingClass == coordpb.ServingClass_SERVING_CLASS_DEGRADED {
			switch syncHealth {
			case coordpb.SyncHealth_SYNC_HEALTH_ROOT_UNAVAILABLE:
				return servingClass, syncHealth, status.Error(codes.FailedPrecondition, errRootUnavailable)
			case coordpb.SyncHealth_SYNC_HEALTH_BOOTSTRAP_REQUIRED:
				return servingClass, syncHealth, status.Error(codes.FailedPrecondition, errBootstrapRequiredBeforeBounded)
			}
		}
	}

	return servingClass, syncHealth, nil
}

// AllocID allocates one or more globally unique ids.
func (s *Service) AllocID(_ context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "alloc id request is nil")
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	if err := s.requireDutyAdmission(rootproto.CoordinatorDutyAllocID); err != nil {
		return nil, err
	}
	first, err := s.reserveIDs(count)
	if err != nil {
		if errors.Is(err, idalloc.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	lease, seal := s.currentCoordinatorLeaseView()
	witness := s.monotoneReplyEvidence(rootproto.CoordinatorDutyAllocID, lease, allocationConsumedFrontier(first, count))
	return &coordpb.AllocIDResponse{
		FirstId:                first,
		Count:                  count,
		CertGeneration:         witness.CertGeneration,
		ConsumedFrontier:       witness.ConsumedFrontier,
		ObservedSealGeneration: seal.CertGeneration,
	}, nil
}

// Tso allocates one or more timestamps.
func (s *Service) Tso(_ context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "tso request is nil")
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	if err := s.requireDutyAdmission(rootproto.CoordinatorDutyTSO); err != nil {
		return nil, err
	}
	first, got, err := s.reserveTSO(count)
	if err != nil {
		if errors.Is(err, idalloc.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	lease, seal := s.currentCoordinatorLeaseView()
	witness := s.monotoneReplyEvidence(rootproto.CoordinatorDutyTSO, lease, allocationConsumedFrontier(first, got))
	return &coordpb.TsoResponse{
		Timestamp:              first,
		Count:                  got,
		CertGeneration:         witness.CertGeneration,
		ConsumedFrontier:       witness.ConsumedFrontier,
		ObservedSealGeneration: seal.CertGeneration,
	}, nil
}

func (s *Service) normalizeRootEvent(event rootevent.Event) (rootevent.Event, error) {
	out := rootevent.CloneEvent(event)
	switch {
	case out.RegionDescriptor != nil:
		desc, err := s.normalizeDescriptorRootEpoch(out.RegionDescriptor.Descriptor)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.RegionDescriptor.Descriptor = desc
	case out.RangeSplit != nil:
		left, err := s.normalizeDescriptorRootEpoch(out.RangeSplit.Left)
		if err != nil {
			return rootevent.Event{}, err
		}
		right, err := s.normalizeDescriptorRootEpoch(out.RangeSplit.Right)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.RangeSplit.Left = left
		out.RangeSplit.Right = right
	case out.RangeMerge != nil:
		merged, err := s.normalizeDescriptorRootEpoch(out.RangeMerge.Merged)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.RangeMerge.Merged = merged
	case out.PeerChange != nil:
		desc, err := s.normalizeDescriptorRootEpoch(out.PeerChange.Region)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.PeerChange.Region = desc
	}
	return out, nil
}

func (s *Service) normalizeDescriptorRootEpoch(desc descriptor.Descriptor) (descriptor.Descriptor, error) {
	if desc.RootEpoch != 0 {
		return desc, nil
	}
	if s != nil && s.cluster != nil {
		current, ok := s.cluster.GetRegionDescriptor(desc.RegionID)
		if ok {
			probe := desc.Clone()
			probe.RootEpoch = current.RootEpoch
			if current.Equal(probe) {
				return probe, nil
			}
		}
	}
	nextEpoch, err := s.nextRootEpoch()
	if err != nil {
		return descriptor.Descriptor{}, err
	}
	desc.RootEpoch = nextEpoch
	return desc, nil
}

func (s *Service) nextRootEpoch() (uint64, error) {
	if s != nil && s.storage != nil {
		snapshot, err := s.storage.Load()
		if err != nil {
			return 0, err
		}
		if snapshot.ClusterEpoch < ^uint64(0) {
			return snapshot.ClusterEpoch + 1, nil
		}
		return snapshot.ClusterEpoch, nil
	}
	var maxEpoch uint64
	if s != nil && s.cluster != nil {
		maxEpoch = s.cluster.MaxDescriptorRevision()
	}
	if maxEpoch < ^uint64(0) {
		return maxEpoch + 1, nil
	}
	return maxEpoch, nil
}

func (s *Service) reserveIDs(count uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	if count == 0 {
		return 0, fmt.Errorf("%w: reserve n must be >= 1", idalloc.ErrInvalidBatch)
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	current := s.ids.Current()
	next, ok := addUint64(current, count)
	if !ok {
		return 0, fmt.Errorf("%w: reserve would overflow", idalloc.ErrInvalidBatch)
	}
	if s.storage != nil && next > s.idWindowHigh {
		windowHigh, ok := addUint64(current, maxUint64(s.effectiveIDWindowSize(), count))
		if !ok {
			windowHigh = next
		}
		if err := s.storage.SaveAllocatorState(windowHigh, s.currentTSOFenceLocked()); err != nil {
			return 0, err
		}
		s.idWindowHigh = windowHigh
	}
	s.ids.Fence(next)
	return current + 1, nil
}

func (s *Service) reserveTSO(count uint64) (uint64, uint64, error) {
	if s == nil {
		return 0, 0, nil
	}
	if count == 0 {
		return 0, 0, fmt.Errorf("%w: tso reserve n must be >= 1", idalloc.ErrInvalidBatch)
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	current := s.tso.Current()
	next, ok := addUint64(current, count)
	if !ok {
		return 0, 0, fmt.Errorf("%w: tso reserve would overflow", idalloc.ErrInvalidBatch)
	}
	if s.storage != nil && next > s.tsoWindowHigh {
		windowHigh, ok := addUint64(current, maxUint64(s.effectiveTSOWindowSize(), count))
		if !ok {
			windowHigh = next
		}
		if err := s.storage.SaveAllocatorState(s.currentIDFenceLocked(), windowHigh); err != nil {
			return 0, 0, err
		}
		s.tsoWindowHigh = windowHigh
	}
	s.tso.Fence(next)
	return current + 1, count, nil
}

func (s *Service) effectiveIDWindowSize() uint64 {
	if s != nil && s.ablation.DisableBudget {
		return ablationUnlimitedWindowSize
	}
	if s == nil || s.idWindowSize == 0 {
		return defaultAllocatorWindowSize
	}
	return s.idWindowSize
}

func (s *Service) effectiveTSOWindowSize() uint64 {
	if s != nil && s.ablation.DisableBudget {
		return ablationUnlimitedWindowSize
	}
	if s == nil || s.tsoWindowSize == 0 {
		return defaultAllocatorWindowSize
	}
	return s.tsoWindowSize
}

func (s *Service) currentIDFenceLocked() uint64 {
	if s == nil {
		return 0
	}
	return maxUint64(s.ids.Current(), s.idWindowHigh)
}

func (s *Service) currentTSOFenceLocked() uint64 {
	if s == nil {
		return 0
	}
	return maxUint64(s.tso.Current(), s.tsoWindowHigh)
}

func (s *Service) fenceIDFromStorage(fence uint64) {
	if s == nil {
		return
	}
	if s.idWindowHigh != 0 && fence <= s.idWindowHigh {
		return
	}
	s.ids.Fence(fence)
	if fence > s.idWindowHigh {
		s.idWindowHigh = fence
	}
}

func (s *Service) fenceTSOFromStorage(fence uint64) {
	if s == nil {
		return
	}
	if s.tsoWindowHigh != 0 && fence <= s.tsoWindowHigh {
		return
	}
	s.tso.Fence(fence)
	if fence > s.tsoWindowHigh {
		s.tsoWindowHigh = fence
	}
}

func addUint64(a, b uint64) (uint64, bool) {
	if ^uint64(0)-a < b {
		return 0, false
	}
	return a + b, true
}

func allocationConsumedFrontier(first, count uint64) uint64 {
	if first == 0 || count == 0 {
		return 0
	}
	last, ok := addUint64(first, count-1)
	if !ok {
		return 0
	}
	return last
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (s *Service) requireLeaderForWrite() error {
	if s == nil || s.storage == nil {
		return nil
	}
	if s.storage.IsLeader() {
		return nil
	}
	leaderID := s.storage.LeaderID()
	if leaderID != 0 {
		return statusNotLeader(leaderID)
	}
	return statusNotLeader(0)
}

func (s *Service) leaseScopedStoreOperations(storeID uint64) []*coordpb.SchedulerOperation {
	if s == nil || !s.coordinatorLeaseEnabled() {
		return s.planStoreOperations(storeID)
	}
	if s.storage != nil && !s.storage.IsLeader() {
		return nil
	}
	if err := s.ensureCoordinatorLease(); err != nil {
		return nil
	}
	return s.planStoreOperations(storeID)
}

func (s *Service) requireDutyAdmission(dutyMask uint32) error {
	if s == nil || !s.coordinatorLeaseEnabled() {
		return nil
	}
	if err := s.ensureCoordinatorLease(); err != nil {
		return translateCoordinatorLeaseError(err)
	}
	return s.preActionGate(preActionDutyAdmission, dutyMask)
}

// RunCoordinatorLeaseLoop keeps the local coordinator lease renewed while ctx
// remains alive. The loop is explicit so callers can decide lifecycle and avoid
// hidden background goroutines in constructors.
func (s *Service) RunCoordinatorLeaseLoop(ctx context.Context) {
	if s == nil || ctx == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if s.storage.IsLeader() {
				_ = s.ensureCoordinatorLease()
			}
			timer.Reset(s.coordinatorLeaseLoopInterval())
		}
	}
}

// ReleaseCoordinatorLease explicitly releases the current rooted coordinator
// lease for the configured holder. It is intended for graceful shutdown.
func (s *Service) ReleaseCoordinatorLease() error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()

	s.leaseMu.RLock()
	holderID := s.coordinatorID
	s.leaseMu.RUnlock()
	if strings.TrimSpace(holderID) == "" {
		return nil
	}

	s.allocMu.Lock()
	handoffFrontiers := controlplane.Frontiers(rootstate.State{
		IDFence:  s.currentIDFenceLocked(),
		TSOFence: s.currentTSOFenceLocked(),
	}, s.currentDescriptorRevision())
	s.allocMu.Unlock()

	if _, err := s.storage.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{
		Kind:             rootproto.CoordinatorLeaseCommandRelease,
		HolderID:         holderID,
		NowUnixNano:      nowUnixNano,
		HandoffFrontiers: handoffFrontiers,
	}); err != nil {
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

// SealCoordinatorLease records one rooted closure point for the current
// authority generation using the frontiers already consumed by this service.
func (s *Service) SealCoordinatorLease() error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if s.ablation.DisableSeal {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	s.allocMu.Lock()
	consumedIDFrontier := s.ids.Current()
	consumedTSOFrontier := s.tso.Current()
	s.allocMu.Unlock()
	return s.applyClosureCommand(
		rootproto.CoordinatorClosureCommandSeal,
		preActionSealCurrentGeneration,
		controlplane.Frontiers(rootstate.State{
			IDFence:  consumedIDFrontier,
			TSOFence: consumedTSOFrontier,
		}, s.currentDescriptorRevision()),
	)
}

// ConfirmCoordinatorClosure explicitly records one rooted audit confirmation
// after a sealed generation has been covered by a successor authority instance.
func (s *Service) ConfirmCoordinatorClosure() error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyClosureCommand(rootproto.CoordinatorClosureCommandConfirm, preActionLifecycleMutation, rootproto.NewCoordinatorDutyFrontiers())
}

// CloseCoordinatorClosure explicitly records that the current successor
// generation has been explicitly closed after rooted closure confirmation.
func (s *Service) CloseCoordinatorClosure() error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyClosureCommand(rootproto.CoordinatorClosureCommandClose, preActionLifecycleMutation, rootproto.NewCoordinatorDutyFrontiers())
}

// ReattachCoordinatorClosure explicitly records that the current successor
// generation has been reattached after rooted close has already landed.
func (s *Service) ReattachCoordinatorClosure() error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if s.ablation.DisableReattach {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyClosureCommand(rootproto.CoordinatorClosureCommandReattach, preActionLifecycleMutation, rootproto.NewCoordinatorDutyFrontiers())
}

func (s *Service) applyClosureCommand(kind rootproto.CoordinatorClosureCommandKind, gate preActionKind, frontiers rootproto.CoordinatorDutyFrontiers) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()

	s.leaseMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.leaseMu.RUnlock()
	if holderID == "" {
		return nil
	}
	if err := s.preActionGate(gate, 0); err != nil {
		return err
	}
	if _, err := s.storage.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{
		Kind:        kind,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	}); err != nil {
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) ensureCoordinatorLease() error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	nowUnixNano, expiresUnixNano, holderID, renewIn, clockSkew := s.leaseCampaignBounds()
	if s.coordinatorLeaseStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.coordinatorLeaseStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}

	s.allocMu.Lock()
	handoffFrontiers := controlplane.Frontiers(rootstate.State{IDFence: s.currentIDFenceLocked(), TSOFence: s.currentTSOFenceLocked()}, s.currentDescriptorRevision())
	s.allocMu.Unlock()
	current, seal := s.currentCoordinatorLeaseView()
	predecessorDigest := rootstate.ResolveCoordinatorLeasePredecessorDigest(current, seal, holderID, nowUnixNano)

	if _, err := s.storage.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{
		Kind:              rootproto.CoordinatorLeaseCommandIssue,
		HolderID:          holderID,
		ExpiresUnixNano:   expiresUnixNano,
		NowUnixNano:       nowUnixNano,
		PredecessorDigest: predecessorDigest,
		HandoffFrontiers:  handoffFrontiers,
	}); err != nil {
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) coordinatorLeaseStillValid(holderID string, nowUnixNano int64, renewIn, clockSkew time.Duration) bool {
	if s == nil {
		return false
	}
	current, seal := s.currentCoordinatorLeaseView()
	if !rootstate.CoordinatorLeaseContinuable(current, seal, holderID, nowUnixNano) {
		return false
	}
	return current.ExpiresUnixNano > nowUnixNano+renewIn.Nanoseconds() &&
		current.ExpiresUnixNano > nowUnixNano+clockSkew.Nanoseconds()
}

func (s *Service) coordinatorLeaseLoopInterval() time.Duration {
	if s == nil {
		return time.Second
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	interval := s.leaseRenewIn / 2
	if interval <= 0 {
		interval = time.Second
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	return interval
}

func (s *Service) coordinatorLeaseEnabled() bool {
	if s == nil {
		return false
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.coordinatorID != "" && s.leaseTTL > 0
}

func (s *Service) currentCoordinatorLease() rootstate.CoordinatorLease {
	if s == nil {
		return rootstate.CoordinatorLease{}
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseView.Lease()
}

func (s *Service) currentCoordinatorLeaseView() (rootstate.CoordinatorLease, rootstate.CoordinatorSeal) {
	if s == nil {
		return rootstate.CoordinatorLease{}, rootstate.CoordinatorSeal{}
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseView.Current()
}

func (s *Service) leaseCampaignBounds() (nowUnixNano, expiresUnixNano int64, holderID string, renewIn, clockSkew time.Duration) {
	if s == nil {
		return 0, 0, "", 0, 0
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	return now.UnixNano(), now.Add(s.leaseTTL).UnixNano(), s.coordinatorID, s.leaseRenewIn, s.leaseClockSkew
}

func translateCoordinatorLeaseError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, rootstate.ErrCoordinatorLeaseHeld) || errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage) || errors.Is(err, rootstate.ErrCoordinatorLeaseLineage) {
		return statusCoordinatorLease(err)
	}
	return status.Error(codes.Internal, "campaign coordinator lease: "+err.Error())
}

func (s *Service) setLastRootReload(err error) {
	if s == nil {
		return
	}
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	if err != nil {
		s.lastRootError = err.Error()
		return
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	s.lastRootReload = nowFn().UnixNano()
	s.lastRootError = ""
}

func (s *Service) requireExpectedClusterEpoch(expected uint64) error {
	if expected == 0 {
		return nil
	}
	current, err := s.currentClusterEpoch()
	if err != nil {
		return status.Error(codes.Internal, "load current cluster epoch: "+err.Error())
	}
	if current == expected {
		return nil
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("pd/meta cluster epoch mismatch (expected=%d current=%d)", expected, current))
}

func (s *Service) currentClusterEpoch() (uint64, error) {
	if s != nil && s.storage != nil {
		snapshot, err := s.storage.Load()
		if err != nil {
			return 0, err
		}
		return snapshot.ClusterEpoch, nil
	}
	var maxEpoch uint64
	if s != nil && s.cluster != nil {
		maxEpoch = s.cluster.MaxDescriptorRevision()
	}
	return maxEpoch, nil
}

func (s *Service) currentDescriptorRevision() uint64 {
	if s == nil || s.cluster == nil {
		return 0
	}
	return s.cluster.MaxDescriptorRevision()
}

type preActionKind uint8

const (
	preActionSealCurrentGeneration preActionKind = iota
	preActionLifecycleMutation
	preActionDutyAdmission
)

func (s *Service) preActionGate(kind preActionKind, dutyMask uint32) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	switch kind {
	case preActionDutyAdmission:
		return s.preActionGateCached(kind, dutyMask)
	default:
		return s.preActionGateStorage(kind, dutyMask)
	}
}

func (s *Service) preActionGateCached(kind preActionKind, dutyMask uint32) error {
	current, seal := s.currentCoordinatorLeaseView()
	return s.validatePreActionLease(kind, dutyMask, current, seal)
}

func (s *Service) preActionGateStorage(kind preActionKind, dutyMask uint32) error {
	current, seal, err := s.currentCoordinatorLeaseViewFromStorage()
	if err != nil {
		return status.Error(codes.Internal, "load rooted snapshot: "+err.Error())
	}
	return s.validatePreActionLease(kind, dutyMask, current, seal)
}

func (s *Service) currentCoordinatorLeaseViewFromStorage() (rootstate.CoordinatorLease, rootstate.CoordinatorSeal, error) {
	if s == nil || s.storage == nil {
		return rootstate.CoordinatorLease{}, rootstate.CoordinatorSeal{}, nil
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return rootstate.CoordinatorLease{}, rootstate.CoordinatorSeal{}, err
	}
	s.refreshLeaseMirror(snapshot)
	return snapshot.CoordinatorLease, snapshot.CoordinatorSeal, nil
}

func (s *Service) validatePreActionLease(kind preActionKind, dutyMask uint32, current rootstate.CoordinatorLease, seal rootstate.CoordinatorSeal) error {
	if s == nil {
		return nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()

	s.leaseMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.leaseMu.RUnlock()
	if holderID == "" {
		return nil
	}

	if current.HolderID == "" {
		return statusCoordinatorLease(fmt.Errorf("%w: no rooted coordinator lease", rootstate.ErrCoordinatorLeaseHeld))
	}
	if current.HolderID != holderID {
		return statusCoordinatorLease(fmt.Errorf("%w: rooted holder=%s local_holder=%s", rootstate.ErrCoordinatorLeaseOwner, current.HolderID, holderID))
	}
	if !current.ActiveAt(nowUnixNano) {
		return statusCoordinatorLease(fmt.Errorf("%w: rooted lease expired generation=%d", rootstate.ErrInvalidCoordinatorLease, current.CertGeneration))
	}

	switch kind {
	case preActionSealCurrentGeneration:
		if rootstate.CoordinatorGenerationSealed(current, seal) {
			return statusCoordinatorLease(fmt.Errorf("%w: generation=%d already sealed", rootstate.ErrCoordinatorLeaseHeld, current.CertGeneration))
		}
	case preActionLifecycleMutation:
		if rootstate.CoordinatorGenerationSealed(current, seal) {
			return statusCoordinatorLease(fmt.Errorf("%w: generation=%d sealed_generation=%d", rootstate.ErrCoordinatorLeaseHeld, current.CertGeneration, seal.CertGeneration))
		}
	case preActionDutyAdmission:
		currentDutyMask := current.DutyMask
		if dutyMask != 0 && currentDutyMask&dutyMask != dutyMask {
			return statusCoordinatorLease(fmt.Errorf("%w: required_duty_mask=%d rooted_duty_mask=%d generation=%d", rootstate.ErrCoordinatorLeaseDuty, dutyMask, currentDutyMask, current.CertGeneration))
		}
		if rootstate.CoordinatorGenerationSealed(current, seal) {
			return statusCoordinatorLease(fmt.Errorf("%w: generation=%d sealed_generation=%d", rootstate.ErrCoordinatorLeaseHeld, current.CertGeneration, seal.CertGeneration))
		}
	}
	return nil
}

func (s *Service) monotoneReplyEvidence(dutyMask uint32, lease rootstate.CoordinatorLease, consumedFrontier uint64) rootproto.ContinuationWitness {
	if s != nil && s.ablation.DisableReplyEvidence {
		return rootproto.NewSuppressedContinuationWitness(dutyMask)
	}
	return rootproto.NewContinuationWitness(dutyMask, lease.CertGeneration, consumedFrontier)
}

func (s *Service) metadataReplyGeneration(certGeneration uint64) uint64 {
	if s != nil && s.ablation.DisableReplyEvidence {
		return rootproto.ContinuationWitnessGenerationSuppressed
	}
	return certGeneration
}
