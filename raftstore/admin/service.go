package admin

import (
	"context"

	"github.com/feichai0017/NoKV/pb"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service exposes raftstore admin operations needed for migration and
// membership management.
type Service struct {
	pb.UnimplementedRaftAdminServer
	store      *store.Store
	snapshot   snapshotpkg.Bridge
	snapshotFS vfs.FS
}

// NewService constructs an admin service bound to one raftstore store.
func NewService(st *store.Store) *Service {
	return &Service{store: st, snapshotFS: vfs.Ensure(nil)}
}

// NewServiceWithSnapshot constructs an admin service with direct access to the
// storage-side snapshot bridge needed for SST export/install.
func NewServiceWithSnapshot(st *store.Store, snapshot snapshotpkg.Bridge, fs vfs.FS) *Service {
	return &Service{
		store:      st,
		snapshot:   snapshot,
		snapshotFS: vfs.Ensure(fs),
	}
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

// RemovePeer issues one raft configuration change removing the specified peer.
func (s *Service) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if req.GetRegionId() == 0 || req.GetPeerId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region_id and peer_id are required")
	}
	if err := s.store.ProposeRemovePeer(req.GetRegionId(), req.GetPeerId()); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &pb.RemovePeerResponse{}, nil
	}
	return &pb.RemovePeerResponse{Region: regionMetaToPB(runtime.Meta)}, nil
}

// TransferLeader requests leader transfer on the specified region.
func (s *Service) TransferLeader(ctx context.Context, req *pb.TransferLeaderRequest) (*pb.TransferLeaderResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if req.GetRegionId() == 0 || req.GetPeerId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region_id and peer_id are required")
	}
	if err := s.store.TransferLeader(req.GetRegionId(), req.GetPeerId()); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &pb.TransferLeaderResponse{}, nil
	}
	return &pb.TransferLeaderResponse{Region: regionMetaToPB(runtime.Meta)}, nil
}

// ExportRegionSnapshot returns the current region snapshot from the leader,
// encoded as one migration-only SST snapshot payload.
func (s *Service) ExportRegionSnapshot(ctx context.Context, req *pb.ExportRegionSnapshotRequest) (*pb.ExportRegionSnapshotResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if req.GetRegionId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region_id is required")
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok || !runtime.Hosted {
		return nil, status.Errorf(codes.NotFound, "region %d is not hosted on this store", req.GetRegionId())
	}
	if !runtime.Leader {
		return nil, status.Errorf(codes.FailedPrecondition, "region %d is not led by this store", req.GetRegionId())
	}
	peerRef, ok := s.store.Peer(runtime.LocalPeerID)
	if !ok || peerRef == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "leader peer %d is not registered", runtime.LocalPeerID)
	}
	snap, err := peerRef.Snapshot()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "export region snapshot: %v", err)
	}
	pbSnap := raftpb.Snapshot(snap)
	if s.snapshot == nil {
		return nil, status.Error(codes.FailedPrecondition, "sst snapshot export is not configured")
	}
	payload, err := s.snapshot.ExportSnapshot(runtime.Meta)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "export sst region snapshot: %v", err)
	}
	pbSnap.Data = payload
	data, err := (&pbSnap).Marshal()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal region snapshot: %v", err)
	}
	return &pb.ExportRegionSnapshotResponse{
		Snapshot: data,
		Region:   regionMetaToPB(runtime.Meta),
	}, nil
}

// InstallRegionSnapshot installs one leader-exported region snapshot on the
// local store. The local peer is bootstrapped on demand from the payload.
func (s *Service) InstallRegionSnapshot(ctx context.Context, req *pb.InstallRegionSnapshotRequest) (*pb.InstallRegionSnapshotResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if len(req.GetSnapshot()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "snapshot is required")
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(req.GetSnapshot()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal region snapshot: %v", err)
	}
	var (
		meta raftmeta.RegionMeta
		err  error
	)
	if s.snapshot == nil {
		return nil, status.Error(codes.FailedPrecondition, "sst snapshot install is not configured")
	}
	metaFile, metaErr := snapshotpkg.ReadSSTPayloadMeta(snap.Data)
	if metaErr != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode sst snapshot payload: %v", metaErr)
	}
	meta, err = s.store.InstallRegionSSTSnapshot(raftpb.Snapshot(snap), metaFile.Region, func() (func() error, error) {
		result, importErr := snapshotpkg.StageSnapshot(s.snapshot, s.store.WorkDir(), snap.Data, s.snapshotFS)
		if importErr != nil {
			return nil, importErr
		}
		if result != nil && len(result.ImportedFileIDs) > 0 {
			return func() error { return result.Rollback(s.snapshot) }, nil
		}
		return nil, nil
	})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(meta.ID)
	if !ok {
		return &pb.InstallRegionSnapshotResponse{}, nil
	}
	return &pb.InstallRegionSnapshotResponse{Region: regionMetaToPB(runtime.Meta)}, nil
}

// RegionRuntimeStatus returns store-local runtime information for one region.
func (s *Service) RegionRuntimeStatus(ctx context.Context, req *pb.RegionRuntimeStatusRequest) (*pb.RegionRuntimeStatusResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if req.GetRegionId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region_id is required")
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &pb.RegionRuntimeStatusResponse{}, nil
	}
	return &pb.RegionRuntimeStatusResponse{
		Known:        true,
		Hosted:       runtime.Hosted,
		LocalPeerId:  runtime.LocalPeerID,
		LeaderPeerId: runtime.LeaderPeerID,
		Leader:       runtime.Leader,
		Region:       regionMetaToPB(runtime.Meta),
		AppliedIndex: runtime.AppliedIndex,
		AppliedTerm:  runtime.AppliedTerm,
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
