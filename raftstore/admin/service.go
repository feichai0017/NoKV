package admin

import (
	"context"

	"github.com/feichai0017/NoKV/pb"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service exposes raftstore admin operations needed for migration and
// membership management.
type Service struct {
	pb.UnimplementedRaftAdminServer
	store *store.Store
}

// NewService constructs an admin service bound to one raftstore store.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

// AddPeer issues one raft configuration change on the region leader.
func (s *Service) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if req.GetRegionId() == 0 || req.GetStoreId() == 0 || req.GetPeerId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region_id, store_id, and peer_id are required")
	}
	if err := s.store.ProposeAddPeer(req.GetRegionId(), raftmeta.PeerMeta{
		StoreID: req.GetStoreId(),
		PeerID:  req.GetPeerId(),
	}); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &pb.AddPeerResponse{}, nil
	}
	return &pb.AddPeerResponse{Region: regionMetaToPB(runtime.Meta)}, nil
}

// RegionStatus returns store-local runtime information for one region.
func (s *Service) RegionStatus(ctx context.Context, req *pb.RegionStatusRequest) (*pb.RegionStatusResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if req.GetRegionId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region_id is required")
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &pb.RegionStatusResponse{}, nil
	}
	return &pb.RegionStatusResponse{
		Known:        true,
		Hosted:       runtime.Hosted,
		LocalPeerId:  runtime.LocalPeerID,
		LeaderPeerId: runtime.LeaderPeerID,
		Leader:       runtime.Leader,
		Region:       regionMetaToPB(runtime.Meta),
	}, nil
}

func regionMetaToPB(meta raftmeta.RegionMeta) *pb.RegionMeta {
	peers := make([]*pb.RegionPeer, 0, len(meta.Peers))
	for _, p := range meta.Peers {
		peers = append(peers, &pb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID})
	}
	return &pb.RegionMeta{
		Id:               meta.ID,
		StartKey:         append([]byte(nil), meta.StartKey...),
		EndKey:           append([]byte(nil), meta.EndKey...),
		EpochVersion:     meta.Epoch.Version,
		EpochConfVersion: meta.Epoch.ConfVersion,
		Peers:            peers,
	}
}
