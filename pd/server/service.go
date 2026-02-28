package server

import (
	"context"
	"errors"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
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
	if req == nil || req.GetRegion() == nil {
		return nil, status.Error(codes.InvalidArgument, "region heartbeat request missing region")
	}
	meta := pbToManifestRegion(req.GetRegion())
	err := s.cluster.UpsertRegionHeartbeat(meta)
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
	return &pb.RemoveRegionResponse{Removed: removed}, nil
}

// GetRegionByKey returns region metadata for the specified key.
func (s *Service) GetRegionByKey(_ context.Context, req *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get region by key request is nil")
	}
	meta, ok := s.cluster.GetRegionByKey(req.GetKey())
	if !ok {
		return &pb.GetRegionByKeyResponse{NotFound: true}, nil
	}
	return &pb.GetRegionByKeyResponse{
		Region:   manifestToPBRegion(meta),
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
	return &pb.TsoResponse{
		Timestamp: first,
		Count:     got,
	}, nil
}

func pbToManifestRegion(meta *pb.RegionMeta) manifest.RegionMeta {
	out := manifest.RegionMeta{
		ID:       meta.GetId(),
		StartKey: append([]byte(nil), meta.GetStartKey()...),
		EndKey:   append([]byte(nil), meta.GetEndKey()...),
		Epoch: manifest.RegionEpoch{
			Version:     meta.GetEpochVersion(),
			ConfVersion: meta.GetEpochConfVersion(),
		},
	}
	if peers := meta.GetPeers(); len(peers) > 0 {
		out.Peers = make([]manifest.PeerMeta, 0, len(peers))
		for _, p := range peers {
			if p == nil {
				continue
			}
			out.Peers = append(out.Peers, manifest.PeerMeta{
				StoreID: p.GetStoreId(),
				PeerID:  p.GetPeerId(),
			})
		}
	}
	return out
}

func manifestToPBRegion(meta manifest.RegionMeta) *pb.RegionMeta {
	out := &pb.RegionMeta{
		Id:               meta.ID,
		StartKey:         append([]byte(nil), meta.StartKey...),
		EndKey:           append([]byte(nil), meta.EndKey...),
		EpochVersion:     meta.Epoch.Version,
		EpochConfVersion: meta.Epoch.ConfVersion,
	}
	if len(meta.Peers) > 0 {
		out.Peers = make([]*pb.RegionPeer, 0, len(meta.Peers))
		for _, p := range meta.Peers {
			out.Peers = append(out.Peers, &pb.RegionPeer{
				StoreId: p.StoreID,
				PeerId:  p.PeerID,
			})
		}
	}
	return out
}
