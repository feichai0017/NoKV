package server

import (
	"context"
	"errors"
	"fmt"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	pdpb "github.com/feichai0017/NoKV/pb/pd"

	"github.com/feichai0017/NoKV/pd/core"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"
	"github.com/feichai0017/NoKV/pd/tso"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

// Service implements the PD-lite gRPC API.
type Service struct {
	pdpb.UnimplementedPDServer

	cluster *core.Cluster
	ids     *core.IDAllocator
	tso     *tso.Allocator
	storage pdstorage.Store
	allocMu sync.Mutex
}

const errNotLeaderPrefix = "pd not leader"

// NewService constructs a PD-lite service.
func NewService(cluster *core.Cluster, ids *core.IDAllocator, tsAlloc *tso.Allocator) *Service {
	if cluster == nil {
		cluster = core.NewCluster()
	}
	if ids == nil {
		ids = core.NewIDAllocator(1)
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

// SetStorage configures optional PD persistence.
//
// When configured, region metadata and allocator states are persisted through
// the storage interface.
func (s *Service) SetStorage(storage pdstorage.Store) {
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
	if err := s.storage.Refresh(); err != nil {
		return err
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return err
	}
	s.cluster.ReplaceRegionSnapshot(snapshot.Descriptors)
	s.ids.Fence(snapshot.Allocator.IDCurrent)
	s.tso.Fence(snapshot.Allocator.TSCurrent)
	return nil
}

// StoreHeartbeat records store-level stats.
func (s *Service) StoreHeartbeat(_ context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "store heartbeat request is nil")
	}
	err := s.cluster.UpsertStoreHeartbeat(core.StoreStats{
		StoreID:   req.GetStoreId(),
		RegionNum: req.GetRegionNum(),
		LeaderNum: req.GetLeaderNum(),
		Capacity:  req.GetCapacity(),
		Available: req.GetAvailable(),
	})
	if err != nil {
		if errors.Is(err, core.ErrInvalidStoreID) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pdpb.StoreHeartbeatResponse{
		Accepted:   true,
		Operations: s.planStoreOperations(req.GetStoreId()),
	}, nil
}

// RegionHeartbeat records region-level metadata.
func (s *Service) RegionHeartbeat(_ context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error) {
	if req == nil || req.GetRegionDescriptor() == nil {
		return nil, status.Error(codes.InvalidArgument, "region heartbeat request missing descriptor")
	}
	desc := metacodec.DescriptorFromProto(req.GetRegionDescriptor())
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	err := s.cluster.ValidateRegionDescriptor(desc)
	if err != nil {
		switch {
		case errors.Is(err, core.ErrInvalidRegionID):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, core.ErrRegionHeartbeatStale), errors.Is(err, core.ErrRegionRangeOverlap):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if s.storage != nil {
		if err := s.storage.PublishRegionDescriptor(desc); err != nil {
			return nil, status.Error(codes.Internal, "publish region descriptor: "+err.Error())
		}
	}
	if err := s.cluster.PublishRegionDescriptor(desc); err != nil {
		return nil, status.Error(codes.Internal, "apply region descriptor after persist: "+err.Error())
	}
	return &pdpb.RegionHeartbeatResponse{Accepted: true}, nil
}

// PublishRootEvent records one explicit rooted topology truth event.
func (s *Service) PublishRootEvent(_ context.Context, req *pdpb.PublishRootEventRequest) (*pdpb.PublishRootEventResponse, error) {
	if req == nil || req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "publish root event request missing event")
	}
	event := metacodec.RootEventFromProto(req.GetEvent())
	if event.Kind == rootevent.KindUnknown {
		return nil, status.Error(codes.InvalidArgument, "publish root event requires known kind")
	}
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	if err := s.cluster.ValidateRootEvent(event); err != nil {
		switch {
		case errors.Is(err, core.ErrInvalidRegionID):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, core.ErrRegionHeartbeatStale), errors.Is(err, core.ErrRegionRangeOverlap):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if s.storage != nil {
		if err := s.storage.AppendRootEvent(event); err != nil {
			return nil, status.Error(codes.Internal, "persist root event: "+err.Error())
		}
	}
	if err := s.cluster.PublishRootEvent(event); err != nil {
		return nil, status.Error(codes.Internal, "apply root event after persist: "+err.Error())
	}
	return &pdpb.PublishRootEventResponse{Accepted: true}, nil
}

// RemoveRegion deletes region metadata from the PD in-memory catalog.
func (s *Service) RemoveRegion(_ context.Context, req *pdpb.RemoveRegionRequest) (*pdpb.RemoveRegionResponse, error) {
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
	if removed && s.storage != nil {
		if err := s.storage.TombstoneRegion(regionID); err != nil {
			return nil, status.Error(codes.Internal, "persist region tombstone: "+err.Error())
		}
	}
	if removed {
		s.cluster.RemoveRegion(regionID)
	}
	return &pdpb.RemoveRegionResponse{Removed: removed}, nil
}

// GetRegionByKey returns region metadata for the specified key.
func (s *Service) GetRegionByKey(_ context.Context, req *pdpb.GetRegionByKeyRequest) (*pdpb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get region by key request is nil")
	}
	desc, ok := s.cluster.GetRegionDescriptorByKey(req.GetKey())
	if !ok {
		return &pdpb.GetRegionByKeyResponse{NotFound: true}, nil
	}
	return &pdpb.GetRegionByKeyResponse{
		RegionDescriptor: metacodec.DescriptorToProto(desc),
		NotFound:         false,
	}, nil
}

// AllocID allocates one or more globally unique ids.
func (s *Service) AllocID(_ context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
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
		if errors.Is(err, core.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	return &pdpb.AllocIDResponse{
		FirstId: first,
		Count:   count,
	}, nil
}

// Tso allocates one or more timestamps.
func (s *Service) Tso(_ context.Context, req *pdpb.TsoRequest) (*pdpb.TsoResponse, error) {
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
		if errors.Is(err, core.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	return &pdpb.TsoResponse{
		Timestamp: first,
		Count:     got,
	}, nil
}

func (s *Service) reserveIDs(count uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	if count == 0 {
		return 0, fmt.Errorf("%w: reserve n must be >= 1", core.ErrInvalidBatch)
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
		return 0, 0, fmt.Errorf("%w: tso reserve n must be >= 1", core.ErrInvalidBatch)
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
