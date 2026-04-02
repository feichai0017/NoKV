package server

import (
	"context"
	"errors"
	"github.com/feichai0017/NoKV/raftstore/descriptor"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"
	"github.com/feichai0017/NoKV/pd/tso"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the PD-lite gRPC API.
type Service struct {
	pb.UnimplementedPDServer

	cluster *core.Cluster
	ids     *core.IDAllocator
	tso     *tso.Allocator
	storage pdstorage.Sink
}

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
func (s *Service) SetStorage(storage pdstorage.Sink) {
	if s == nil {
		return
	}
	s.storage = storage
}

// StoreHeartbeat records store-level stats.
func (s *Service) StoreHeartbeat(_ context.Context, req *pb.StoreHeartbeatRequest) (*pb.StoreHeartbeatResponse, error) {
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
	return &pb.StoreHeartbeatResponse{
		Accepted:   true,
		Operations: s.planStoreOperations(req.GetStoreId()),
	}, nil
}

// RegionHeartbeat records region-level metadata.
func (s *Service) RegionHeartbeat(_ context.Context, req *pb.RegionHeartbeatRequest) (*pb.RegionHeartbeatResponse, error) {
	if req == nil || req.GetRegionDescriptor() == nil {
		return nil, status.Error(codes.InvalidArgument, "region heartbeat request missing descriptor")
	}
	desc := descriptor.FromProto(req.GetRegionDescriptor())
	err := s.cluster.PublishRegionDescriptor(desc)
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
	return &pb.RegionHeartbeatResponse{Accepted: true}, nil
}

// RemoveRegion deletes region metadata from the PD in-memory catalog.
func (s *Service) RemoveRegion(_ context.Context, req *pb.RemoveRegionRequest) (*pb.RemoveRegionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "remove region request is nil")
	}
	regionID := req.GetRegionId()
	if regionID == 0 {
		return nil, status.Error(codes.InvalidArgument, "remove region requires region_id > 0")
	}
	removed := s.cluster.RemoveRegion(regionID)
	if removed && s.storage != nil {
		if err := s.storage.TombstoneRegion(regionID); err != nil {
			return nil, status.Error(codes.Internal, "persist region tombstone: "+err.Error())
		}
	}
	return &pb.RemoveRegionResponse{Removed: removed}, nil
}

// GetRegionByKey returns region metadata for the specified key.
func (s *Service) GetRegionByKey(_ context.Context, req *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get region by key request is nil")
	}
	desc, ok := s.cluster.GetRegionDescriptorByKey(req.GetKey())
	if !ok {
		return &pb.GetRegionByKeyResponse{NotFound: true}, nil
	}
		return &pb.GetRegionByKeyResponse{
			Region:   descriptorToRoutePB(desc),
			NotFound: false,
		}, nil
}

// AllocID allocates one or more globally unique ids.
func (s *Service) AllocID(_ context.Context, req *pb.AllocIDRequest) (*pb.AllocIDResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "alloc id request is nil")
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	first, _, err := s.ids.Reserve(count)
	if err != nil {
		if errors.Is(err, core.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.persistAllocatorState(); err != nil {
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	return &pb.AllocIDResponse{
		FirstId: first,
		Count:   count,
	}, nil
}

// Tso allocates one or more timestamps.
func (s *Service) Tso(_ context.Context, req *pb.TsoRequest) (*pb.TsoResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "tso request is nil")
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	first, got, err := s.tso.Reserve(count)
	if err != nil {
		if errors.Is(err, core.ErrInvalidBatch) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.persistAllocatorState(); err != nil {
		return nil, status.Error(codes.Internal, "persist allocator state: "+err.Error())
	}
	return &pb.TsoResponse{
		Timestamp: first,
		Count:     got,
	}, nil
}

func (s *Service) persistAllocatorState() error {
	if s == nil || s.storage == nil {
		return nil
	}
	return s.storage.SaveAllocatorState(s.ids.Current(), s.tso.Current())
}

// descriptorToRoutePB is the remaining RPC-boundary adapter for clients that
// still fetch route results as pb.RegionMeta. PD runtime stays descriptor-first.
func descriptorToRoutePB(desc descriptor.Descriptor) *pb.RegionMeta {
	out := &pb.RegionMeta{
		Id:               desc.RegionID,
		StartKey:         append([]byte(nil), desc.StartKey...),
		EndKey:           append([]byte(nil), desc.EndKey...),
		EpochVersion:     desc.Epoch.Version,
		EpochConfVersion: desc.Epoch.ConfVersion,
	}
	if len(desc.Peers) > 0 {
		out.Peers = make([]*pb.RegionPeer, 0, len(desc.Peers))
		for _, p := range desc.Peers {
			out.Peers = append(out.Peers, &pb.RegionPeer{
				StoreId: p.StoreID,
				PeerId:  p.PeerID,
			})
		}
	}
	return out
}
