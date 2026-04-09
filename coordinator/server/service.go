package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/coordinator/tso"
	pdview "github.com/feichai0017/NoKV/coordinator/view"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

// Service implements the Coordinator gRPC API.
type Service struct {
	coordpb.UnimplementedCoordinatorServer

	cluster *catalog.Cluster
	ids     *idalloc.IDAllocator
	tso     *tso.Allocator
	storage coordstorage.Store
	allocMu sync.Mutex
}

const errNotLeaderPrefix = "coordinator not leader"
const errRootUnavailable = "coordinator root unavailable"

// NewService constructs a Coordinator service.
func NewService(cluster *catalog.Cluster, ids *idalloc.IDAllocator, tsAlloc *tso.Allocator) *Service {
	if cluster == nil {
		cluster = catalog.NewCluster()
	}
	if ids == nil {
		ids = idalloc.NewIDAllocator(1)
	}
	if tsAlloc == nil {
		tsAlloc = tso.NewAllocator(1)
	}
	return &Service{
		cluster: cluster,
		ids:     ids,
		tso:     tsAlloc,
	}
}

// SetStorage configures optional Coordinator persistence.
//
// When configured, region metadata and allocator states are persisted through
// the storage interface.
func (s *Service) SetStorage(storage coordstorage.Store) {
	if s == nil {
		return
	}
	s.storage = storage
}

// RefreshFromStorage refreshes rooted durable state into the in-memory service
// view and fences allocator state so a future leader cannot allocate stale ids.
func (s *Service) RefreshFromStorage() error {
	if s == nil || s.storage == nil {
		return nil
	}
	snapshot, err := s.reloadRootedView(true)
	if err != nil {
		return err
	}
	s.ids.Fence(snapshot.Allocator.IDCurrent)
	s.tso.Fence(snapshot.Allocator.TSCurrent)
	return nil
}

// ReloadFromStorage reloads the in-memory rooted view from the storage cache
// without forcing the underlying rooted backend to refresh first.
func (s *Service) ReloadFromStorage() error {
	if s == nil || s.storage == nil {
		return nil
	}
	snapshot, err := s.reloadRootedView(false)
	if err != nil {
		return err
	}
	s.ids.Fence(snapshot.Allocator.IDCurrent)
	s.tso.Fence(snapshot.Allocator.TSCurrent)
	return nil
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
	return &coordpb.StoreHeartbeatResponse{
		Accepted:   true,
		Operations: s.planStoreOperations(req.GetStoreId()),
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
	event := metacodec.RootEventFromProto(req.GetEvent())
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
	if err := s.requireExpectedClusterEpoch(req.GetExpectedClusterEpoch()); err != nil {
		return nil, err
	}
	skip, err := s.guardRootEventLifecycle(event)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if skip {
		return &coordpb.PublishRootEventResponse{Accepted: true}, nil
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
		return &coordpb.PublishRootEventResponse{Accepted: true}, nil
	}
	if err := s.cluster.PublishRootEvent(event); err != nil {
		return nil, status.Error(codes.Internal, "apply root event after persist: "+err.Error())
	}
	return &coordpb.PublishRootEventResponse{Accepted: true}, nil
}

func (s *Service) guardRootEventLifecycle(event rootevent.Event) (bool, error) {
	if s == nil || s.storage == nil {
		return false, nil
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return false, fmt.Errorf("load rooted snapshot: %w", err)
	}
	decision, err := rootstate.EvaluateRootEventLifecycle(rootstate.Snapshot{
		Descriptors:         snapshot.Descriptors,
		PendingPeerChanges:  snapshot.PendingPeerChanges,
		PendingRangeChanges: snapshot.PendingRangeChanges,
	}, event)
	return decision == rootstate.RootEventLifecycleSkip, err
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
	if err := s.requireExpectedClusterEpoch(req.GetExpectedClusterEpoch()); err != nil {
		return nil, err
	}
	removed := s.cluster.HasRegion(regionID)
	if removed && s.storage != nil {
		if err := s.storage.AppendRootEvent(rootevent.RegionTombstoned(regionID)); err != nil {
			return nil, status.Error(codes.Internal, "persist region tombstone: "+err.Error())
		}
		if _, err := s.reloadRootedView(false); err != nil {
			return nil, status.Error(codes.Internal, "reload rooted view: "+err.Error())
		}
		return &coordpb.RemoveRegionResponse{Removed: true}, nil
	}
	if removed {
		s.cluster.RemoveRegion(regionID)
	}
	return &coordpb.RemoveRegionResponse{Removed: removed}, nil
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

// GetRegionByKey returns region metadata for the specified key.
func (s *Service) GetRegionByKey(_ context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get region by key request is nil")
	}
	freshness := normalizeFreshness(req.GetFreshness())
	state, err := s.currentReadState()
	if err != nil {
		if freshness == coordpb.Freshness_FRESHNESS_STRONG || freshness == coordpb.Freshness_FRESHNESS_BOUNDED {
			return nil, status.Error(codes.FailedPrecondition, errRootUnavailable)
		}
		state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE
	}
	if freshness == coordpb.Freshness_FRESHNESS_STRONG && !state.servedByLeader {
		return nil, s.notLeaderError()
	}
	if freshness == coordpb.Freshness_FRESHNESS_STRONG && state.rootLag > 0 {
		return nil, status.Error(codes.FailedPrecondition, "root lag exceeds strong freshness")
	}
	required := rootTokenFromProto(req.GetRequiredRootToken())
	if !rootTokenSatisfied(state.servedToken, required) {
		return nil, status.Error(codes.FailedPrecondition, "required rooted token not satisfied")
	}
	if freshness == coordpb.Freshness_FRESHNESS_BOUNDED && !boundedLagSatisfied(state.rootLag, req.GetMaxRootLag()) {
		return nil, status.Error(codes.FailedPrecondition, "root lag exceeds bound")
	}
	desc, ok := s.cluster.GetRegionDescriptorByKey(req.GetKey())
	if !ok {
		return &coordpb.GetRegionByKeyResponse{
			NotFound:         true,
			ServedRootToken:  rootTokenToProto(state.servedToken),
			ServedFreshness:  freshness,
			DegradedMode:     state.degraded,
			ServedByLeader:   state.servedByLeader,
			CurrentRootToken: rootTokenToProto(state.currentToken),
			RootLag:          state.rootLag,
		}, nil
	}
	return &coordpb.GetRegionByKeyResponse{
		RegionDescriptor: metacodec.DescriptorToProto(desc),
		NotFound:         false,
		ServedRootToken:  rootTokenToProto(state.servedToken),
		ServedFreshness:  freshness,
		DegradedMode:     state.degraded,
		ServedByLeader:   state.servedByLeader,
		CurrentRootToken: rootTokenToProto(state.currentToken),
		RootLag:          state.rootLag,
	}, nil
}

type readState struct {
	servedToken    rootstorage.TailToken
	currentToken   rootstorage.TailToken
	rootLag        uint64
	degraded       coordpb.DegradedMode
	servedByLeader bool
}

func (s *Service) currentReadState() (readState, error) {
	if s == nil {
		return readState{degraded: coordpb.DegradedMode_DEGRADED_MODE_HEALTHY, servedByLeader: true}, nil
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
	}
	if s.storage == nil {
		state.rootLag = rootLag(state.currentToken, state.servedToken)
		return state, nil
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE
		return state, err
	}
	state.currentToken = snapshot.RootToken
	state.rootLag = rootLag(state.currentToken, state.servedToken)
	if state.rootLag > 0 {
		state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING
	}
	return state, nil
}

func (s *Service) notLeaderError() error {
	if s == nil || s.storage == nil {
		return status.Error(codes.FailedPrecondition, errNotLeaderPrefix)
	}
	leaderID := s.storage.LeaderID()
	if leaderID == 0 {
		return status.Error(codes.FailedPrecondition, errNotLeaderPrefix)
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("%s (leader_id=%d)", errNotLeaderPrefix, leaderID))
}

func normalizeFreshness(f coordpb.Freshness) coordpb.Freshness {
	switch f {
	case coordpb.Freshness_FRESHNESS_STRONG,
		coordpb.Freshness_FRESHNESS_BOUNDED,
		coordpb.Freshness_FRESHNESS_BEST_EFFORT:
		return f
	default:
		return coordpb.Freshness_FRESHNESS_BEST_EFFORT
	}
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
	if !rootTokenSatisfied(current, served) {
		return 0
	}
	if current.Revision > 0 || served.Revision > 0 {
		if current.Revision >= served.Revision {
			return current.Revision - served.Revision
		}
		return 0
	}
	if rootstate.CursorAfter(current.Cursor, served.Cursor) {
		return 1
	}
	return 0
}

func boundedLagSatisfied(lag, bound uint64) bool {
	if bound == 0 {
		return lag == 0
	}
	return lag <= bound
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
	first, err := s.reserveIDs(count)
	if err != nil {
		if errors.Is(err, idalloc.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	return &coordpb.AllocIDResponse{
		FirstId: first,
		Count:   count,
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
	first, got, err := s.reserveTSO(count)
	if err != nil {
		if errors.Is(err, idalloc.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	return &coordpb.TsoResponse{
		Timestamp: first,
		Count:     got,
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
		for _, region := range s.cluster.RegionSnapshot() {
			if region.Descriptor.RootEpoch > maxEpoch {
				maxEpoch = region.Descriptor.RootEpoch
			}
		}
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
	next := current + count
	if s.storage != nil {
		if err := s.storage.SaveAllocatorState(next, s.tso.Current()); err != nil {
			return 0, err
		}
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
	next := current + count
	if s.storage != nil {
		if err := s.storage.SaveAllocatorState(s.ids.Current(), next); err != nil {
			return 0, 0, err
		}
	}
	s.tso.Fence(next)
	return current + 1, count, nil
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
		return status.Error(codes.FailedPrecondition, fmt.Sprintf("%s (leader_id=%d)", errNotLeaderPrefix, leaderID))
	}
	return status.Error(codes.FailedPrecondition, errNotLeaderPrefix)
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
		for _, region := range s.cluster.RegionSnapshot() {
			if region.Descriptor.RootEpoch > maxEpoch {
				maxEpoch = region.Descriptor.RootEpoch
			}
		}
	}
	return maxEpoch, nil
}
