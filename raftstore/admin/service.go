package admin

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/feichai0017/NoKV/pb"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const snapshotStreamChunkSize = 64 << 10

// Service exposes raftstore admin operations needed for migration and
// membership management.
type Service struct {
	pb.UnimplementedRaftAdminServer
	store    *store.Store
	snapshot snapshotpkg.SnapshotStore
}

// NewService constructs an admin service bound to one raftstore store.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

// NewServiceWithSnapshot constructs an admin service with direct access to the
// storage-side snapshot bridge needed for SST export/import.
func NewServiceWithSnapshot(st *store.Store, snapshot snapshotpkg.SnapshotStore) *Service {
	return &Service{store: st, snapshot: snapshot}
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
	if err := s.store.ProposeAddPeer(req.GetRegionId(), raftmeta.PeerMeta{StoreID: req.GetStoreId(), PeerID: req.GetPeerId()}); err != nil {
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
	runtime, snap, err := s.exportRegionSnapshot(req.GetRegionId())
	if err != nil {
		return nil, err
	}
	payload, err := s.snapshot.ExportSnapshot(runtime.Meta)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "export sst region snapshot: %v", err)
	}
	snap.Data = payload
	data, err := (&snap).Marshal()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal region snapshot: %v", err)
	}
	return &pb.ExportRegionSnapshotResponse{Snapshot: data, Region: regionMetaToPB(runtime.Meta)}, nil
}

// ExportRegionSnapshotStream streams one migration-only SST snapshot payload.
// The first message carries the raft snapshot header and region metadata.
func (s *Service) ExportRegionSnapshotStream(req *pb.ExportRegionSnapshotStreamRequest, stream pb.RaftAdmin_ExportRegionSnapshotStreamServer) error {
	runtime, snap, err := s.exportRegionSnapshot(req.GetRegionId())
	if err != nil {
		return err
	}
	headerSnap := snap
	headerSnap.Data = nil
	header, err := (&headerSnap).Marshal()
	if err != nil {
		return status.Errorf(codes.Internal, "marshal region snapshot header: %v", err)
	}
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, writeErr := s.snapshot.ExportSnapshotTo(pw, runtime.Meta)
		_ = pw.CloseWithError(writeErr)
		errCh <- writeErr
	}()
	buf := make([]byte, snapshotStreamChunkSize)
	first := true
	for {
		n, readErr := pr.Read(buf)
		if n > 0 || first {
			resp := &pb.ExportRegionSnapshotStreamResponse{Chunk: append([]byte(nil), buf[:n]...)}
			if first {
				resp.SnapshotHeader = header
				resp.Region = regionMetaToPB(runtime.Meta)
				first = false
			}
			if err := stream.Send(resp); err != nil {
				_ = pr.Close()
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "export region snapshot stream: %v", readErr)
		}
	}
	if writeErr := <-errCh; writeErr != nil {
		return status.Errorf(codes.Internal, "export sst region snapshot stream: %v", writeErr)
	}
	return nil
}

// ImportRegionSnapshot imports one leader-exported region snapshot on the local
// store. The local peer is bootstrapped on demand from the payload.
func (s *Service) ImportRegionSnapshot(ctx context.Context, req *pb.ImportRegionSnapshotRequest) (*pb.ImportRegionSnapshotResponse, error) {
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
	meta, err := s.importRegionSnapshot(snap, nil)
	if err != nil {
		return nil, err
	}
	return &pb.ImportRegionSnapshotResponse{Region: regionMetaToPB(meta)}, nil
}

// ImportRegionSnapshotStream imports one leader-exported region snapshot from a
// streamed payload. The first chunk must carry the raft snapshot header and
// region metadata.
func (s *Service) ImportRegionSnapshotStream(stream pb.RaftAdmin_ImportRegionSnapshotStreamServer) error {
	if s == nil || s.store == nil {
		return status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if s.snapshot == nil {
		return status.Error(codes.FailedPrecondition, "sst snapshot import is not configured")
	}
	first, err := stream.Recv()
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "snapshot stream is empty")
	}
	if err != nil {
		return err
	}
	if len(first.GetSnapshotHeader()) == 0 {
		return status.Error(codes.InvalidArgument, "snapshot_header is required")
	}
	if first.GetRegion() == nil {
		return status.Error(codes.InvalidArgument, "region is required")
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(first.GetSnapshotHeader()); err != nil {
		return status.Errorf(codes.InvalidArgument, "unmarshal region snapshot header: %v", err)
	}
	meta, err := regionMetaFromPB(first.GetRegion())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "decode region snapshot metadata: %v", err)
	}
	pr, pw := io.Pipe()
	resultCh := make(chan struct {
		meta raftmeta.RegionMeta
		err  error
	}, 1)
	go func() {
		installed, importErr := s.importRegionSnapshot(snap, &streamedImport{meta: meta, reader: pr})
		resultCh <- struct {
			meta raftmeta.RegionMeta
			err  error
		}{meta: installed, err: importErr}
	}()
	writeChunk := func(chunk []byte) error {
		if len(chunk) == 0 {
			return nil
		}
		_, err := pw.Write(chunk)
		return err
	}
	if err := writeChunk(first.GetChunk()); err != nil {
		_ = pw.CloseWithError(err)
		outcome := <-resultCh
		if outcome.err != nil {
			return outcome.err
		}
		return status.Errorf(codes.Internal, "write region snapshot stream: %v", err)
	}
	for {
		req, recvErr := stream.Recv()
		if recvErr == io.EOF {
			_ = pw.Close()
			break
		}
		if recvErr != nil {
			_ = pw.CloseWithError(recvErr)
			outcome := <-resultCh
			if outcome.err != nil {
				return outcome.err
			}
			return recvErr
		}
		if len(req.GetSnapshotHeader()) != 0 || req.GetRegion() != nil {
			streamErr := fmt.Errorf("snapshot header repeated")
			_ = pw.CloseWithError(streamErr)
			<-resultCh
			return status.Error(codes.InvalidArgument, "snapshot header may only appear in the first chunk")
		}
		if err := writeChunk(req.GetChunk()); err != nil {
			_ = pw.CloseWithError(err)
			outcome := <-resultCh
			if outcome.err != nil {
				return outcome.err
			}
			return status.Errorf(codes.Internal, "write region snapshot stream: %v", err)
		}
	}
	outcome := <-resultCh
	if outcome.err != nil {
		return outcome.err
	}
	return stream.SendAndClose(&pb.ImportRegionSnapshotResponse{Region: regionMetaToPB(outcome.meta)})
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

func regionMetaFromPB(meta *pb.RegionMeta) (raftmeta.RegionMeta, error) {
	if meta == nil {
		return raftmeta.RegionMeta{}, fmt.Errorf("region metadata is nil")
	}
	peers := make([]raftmeta.PeerMeta, 0, len(meta.GetPeers()))
	for _, peerMeta := range meta.GetPeers() {
		if peerMeta.GetStoreId() == 0 || peerMeta.GetPeerId() == 0 {
			return raftmeta.RegionMeta{}, fmt.Errorf("region peer metadata is incomplete")
		}
		peers = append(peers, raftmeta.PeerMeta{StoreID: peerMeta.GetStoreId(), PeerID: peerMeta.GetPeerId()})
	}
	return raftmeta.RegionMeta{
		ID:       meta.GetId(),
		StartKey: append([]byte(nil), meta.GetStartKey()...),
		EndKey:   append([]byte(nil), meta.GetEndKey()...),
		Epoch: raftmeta.RegionEpoch{
			Version:     meta.GetEpochVersion(),
			ConfVersion: meta.GetEpochConfVersion(),
		},
		Peers: peers,
	}, nil
}

func matchesSnapshotRegion(header, payload raftmeta.RegionMeta) bool {
	if header.ID != payload.ID {
		return false
	}
	if !bytes.Equal(header.StartKey, payload.StartKey) {
		return false
	}
	if !bytes.Equal(header.EndKey, payload.EndKey) {
		return false
	}
	if header.Epoch != payload.Epoch {
		return false
	}
	if len(header.Peers) != len(payload.Peers) {
		return false
	}
	for i := range header.Peers {
		if header.Peers[i] != payload.Peers[i] {
			return false
		}
	}
	return true
}

func (s *Service) exportRegionSnapshot(regionID uint64) (store.RegionRuntimeStatus, raftpb.Snapshot, error) {
	if s == nil || s.store == nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, status.Error(codes.FailedPrecondition, "raft admin service not configured")
	}
	if regionID == 0 {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, status.Error(codes.InvalidArgument, "region_id is required")
	}
	runtime, ok := s.store.RegionRuntimeStatus(regionID)
	if !ok || !runtime.Hosted {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, status.Errorf(codes.NotFound, "region %d is not hosted on this store", regionID)
	}
	if !runtime.Leader {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, status.Errorf(codes.FailedPrecondition, "region %d is not led by this store", regionID)
	}
	peerRef, ok := s.store.Peer(runtime.LocalPeerID)
	if !ok || peerRef == nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, status.Errorf(codes.FailedPrecondition, "leader peer %d is not registered", runtime.LocalPeerID)
	}
	snap, err := peerRef.Snapshot()
	if err != nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, status.Errorf(codes.Internal, "export region snapshot: %v", err)
	}
	if s.snapshot == nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, status.Error(codes.FailedPrecondition, "sst snapshot export is not configured")
	}
	return runtime, raftpb.Snapshot(snap), nil
}

type streamedImport struct {
	meta   raftmeta.RegionMeta
	reader io.Reader
}

func (s *Service) importRegionSnapshot(snap raftpb.Snapshot, streamed *streamedImport) (raftmeta.RegionMeta, error) {
	if s.snapshot == nil {
		return raftmeta.RegionMeta{}, status.Error(codes.FailedPrecondition, "sst snapshot import is not configured")
	}
	var meta raftmeta.RegionMeta
	if streamed == nil {
		metaFile, metaErr := snapshotpkg.ReadPayloadMeta(snap.Data)
		if metaErr != nil {
			return raftmeta.RegionMeta{}, status.Errorf(codes.InvalidArgument, "decode sst snapshot payload: %v", metaErr)
		}
		meta = metaFile.Region
	} else {
		meta = streamed.meta
	}
	installImport := func() (*snapshotpkg.ImportResult, error) {
		if streamed != nil {
			return s.snapshot.ImportSnapshotFrom(streamed.reader)
		}
		return s.snapshot.ImportSnapshot(snap.Data)
	}
	installed, err := s.store.InstallRegionSSTSnapshot(raftpb.Snapshot(snap), meta, func() (func() error, error) {
		result, importErr := installImport()
		if importErr != nil {
			return nil, importErr
		}
		if result == nil {
			return nil, nil
		}
		if !matchesSnapshotRegion(meta, result.Meta.Region) {
			if rollbackErr := result.Rollback(); rollbackErr != nil {
				return nil, fmt.Errorf("region snapshot metadata mismatch and rollback failed: %w", rollbackErr)
			}
			return nil, fmt.Errorf("region snapshot metadata mismatch: header=%+v payload=%+v", meta, result.Meta.Region)
		}
		if len(result.ImportedFileIDs) == 0 {
			return nil, nil
		}
		return result.Rollback, nil
	})
	if err != nil {
		return raftmeta.RegionMeta{}, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(installed.ID)
	if !ok {
		return installed, nil
	}
	return runtime.Meta, nil
}
