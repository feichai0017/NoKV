package admin

import (
	"bytes"
	"context"
	"io"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/metadata"
)

type noopTransport struct{}

type noopSnapshotStore struct{}

func (noopTransport) Send(context.Context, myraft.Message) {}

func (noopSnapshotStore) ExportSnapshot(raftmeta.RegionMeta) ([]byte, error) {
	return nil, nil
}

func (noopSnapshotStore) ImportSnapshot([]byte) (*snapshotpkg.ImportResult, error) {
	return nil, nil
}

func (noopSnapshotStore) ExportSnapshotTo(io.Writer, raftmeta.RegionMeta) (snapshotpkg.Meta, error) {
	return snapshotpkg.Meta{}, nil
}

func (noopSnapshotStore) ImportSnapshotFrom(io.Reader) (*snapshotpkg.ImportResult, error) {
	return nil, nil
}

func openAdminTestDBWithTweak(t *testing.T, dir string, tweak func(*NoKV.Options)) (*NoKV.DB, *raftmeta.Store) {
	t.Helper()
	localMeta, err := raftmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	if tweak != nil {
		tweak(opt)
	}
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db, localMeta
}

func testSSTExport(db *NoKV.DB) peer.SnapshotExportFunc {
	return func(region raftmeta.RegionMeta) ([]byte, error) {
		return db.ExportSnapshot(region)
	}
}

func testSSTApply(db *NoKV.DB) peer.SnapshotApplyFunc {
	return func(payload []byte) (raftmeta.RegionMeta, error) {
		result, err := db.ImportSnapshot(payload)
		if err != nil {
			return raftmeta.RegionMeta{}, err
		}
		return result.Meta.Region, nil
	}
}

func TestServiceExportsAndInstallsRegionSnapshot(t *testing.T) {
	sourceDir := t.TempDir()
	sourceDB, sourceMeta := openAdminTestDBWithTweak(t, sourceDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	defer func() {
		require.NoError(t, sourceDB.Close())
		require.NoError(t, sourceMeta.Close())
	}()

	valueBacked := bytes.Repeat([]byte("z"), 4096)
	entry := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("alpha"), 9, valueBacked, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{entry}))

	region := raftmeta.RegionMeta{
		ID:       22,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    raftmeta.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []raftmeta.PeerMeta{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: raftmeta.RegionStateRunning,
	}
	require.NoError(t, sourceMeta.SaveRegion(region))

	sourceStorage, err := sourceDB.RaftLog().Open(region.ID, sourceMeta)
	require.NoError(t, err)
	require.NoError(t, sourceStorage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{101},
			},
		},
	}))

	sourceStore := store.NewStore(store.Config{
		StoreID:   1,
		LocalMeta: sourceMeta,
		PeerBuilder: func(meta raftmeta.RegionMeta) (*peer.Config, error) {
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              101,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:      noopTransport{},
				Apply:          func([]myraft.Entry) error { return nil },
				SnapshotExport: testSSTExport(sourceDB),
				Storage:        sourceStorage,
				GroupID:        meta.ID,
				Region:         raftmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer sourceStore.Close()

	sourcePeerCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:      noopTransport{},
		Apply:          func([]myraft.Entry) error { return nil },
		SnapshotExport: testSSTExport(sourceDB),
		Storage:        sourceStorage,
		GroupID:        region.ID,
		Region:         raftmeta.CloneRegionMetaPtr(&region),
	}
	_, err = sourceStore.StartPeer(sourcePeerCfg, nil)
	require.NoError(t, err)

	sourcePeer, ok := sourceStore.Peer(101)
	require.True(t, ok)
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool {
		status, ok := sourceStore.RegionRuntimeStatus(region.ID)
		return ok && status.Leader
	}, 2_000_000_000, 20_000_000)

	targetDir := t.TempDir()
	targetDB, targetMeta := openAdminTestDBWithTweak(t, targetDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	defer func() {
		require.NoError(t, targetDB.Close())
		require.NoError(t, targetMeta.Close())
	}()
	targetStore := store.NewStore(store.Config{
		StoreID:   2,
		LocalMeta: targetMeta,
		PeerBuilder: func(meta raftmeta.RegionMeta) (*peer.Config, error) {
			storage, err := targetDB.RaftLog().Open(meta.ID, targetMeta)
			require.NoError(t, err)
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              201,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:     noopTransport{},
				Apply:         func([]myraft.Entry) error { return nil },
				SnapshotApply: testSSTApply(targetDB),
				Storage:       storage,
				GroupID:       meta.ID,
				Region:        raftmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer targetStore.Close()

	sourceSvc := NewServiceWithSnapshot(sourceStore, sourceDB)
	targetSvc := NewServiceWithSnapshot(targetStore, targetDB)

	exported, err := sourceSvc.ExportRegionSnapshot(context.Background(), &pb.ExportRegionSnapshotRequest{
		RegionId: region.ID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, exported.GetSnapshot())

	installed, err := targetSvc.ImportRegionSnapshot(context.Background(), &pb.ImportRegionSnapshotRequest{
		Snapshot: exported.GetSnapshot(),
	})
	require.NoError(t, err)
	require.Equal(t, region.ID, installed.GetRegion().GetId())

	status, ok := targetStore.RegionRuntimeStatus(region.ID)
	require.True(t, ok)
	require.True(t, status.Hosted)
	require.Equal(t, uint64(201), status.LocalPeerID)
	require.GreaterOrEqual(t, status.AppliedIndex, uint64(1))

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("alpha"), 9)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, valueBacked, got.Value)
}

type exportRegionSnapshotStreamCapture struct {
	ctx    context.Context
	chunks []*pb.ExportRegionSnapshotStreamResponse
}

func (s *exportRegionSnapshotStreamCapture) Send(resp *pb.ExportRegionSnapshotStreamResponse) error {
	copyResp := &pb.ExportRegionSnapshotStreamResponse{
		SnapshotHeader: append([]byte(nil), resp.GetSnapshotHeader()...),
		Chunk:          append([]byte(nil), resp.GetChunk()...),
	}
	if resp.GetRegion() != nil {
		copyResp.Region = &pb.RegionMeta{
			Id:               resp.GetRegion().GetId(),
			StartKey:         append([]byte(nil), resp.GetRegion().GetStartKey()...),
			EndKey:           append([]byte(nil), resp.GetRegion().GetEndKey()...),
			EpochVersion:     resp.GetRegion().GetEpochVersion(),
			EpochConfVersion: resp.GetRegion().GetEpochConfVersion(),
			Peers:            append([]*pb.RegionPeer(nil), resp.GetRegion().GetPeers()...),
		}
	}
	s.chunks = append(s.chunks, copyResp)
	return nil
}

func (s *exportRegionSnapshotStreamCapture) SetHeader(metadata.MD) error  { return nil }
func (s *exportRegionSnapshotStreamCapture) SendHeader(metadata.MD) error { return nil }
func (s *exportRegionSnapshotStreamCapture) SetTrailer(metadata.MD)       {}
func (s *exportRegionSnapshotStreamCapture) Context() context.Context     { return s.ctx }
func (s *exportRegionSnapshotStreamCapture) SendMsg(any) error            { return nil }
func (s *exportRegionSnapshotStreamCapture) RecvMsg(any) error            { return nil }

type importRegionSnapshotStreamFeed struct {
	ctx  context.Context
	reqs []*pb.ImportRegionSnapshotStreamRequest
	idx  int
	resp *pb.ImportRegionSnapshotResponse
}

func (s *importRegionSnapshotStreamFeed) Recv() (*pb.ImportRegionSnapshotStreamRequest, error) {
	if s.idx >= len(s.reqs) {
		return nil, io.EOF
	}
	req := s.reqs[s.idx]
	s.idx++
	return req, nil
}

func (s *importRegionSnapshotStreamFeed) SendAndClose(resp *pb.ImportRegionSnapshotResponse) error {
	s.resp = resp
	return nil
}

func (s *importRegionSnapshotStreamFeed) SetHeader(metadata.MD) error  { return nil }
func (s *importRegionSnapshotStreamFeed) SendHeader(metadata.MD) error { return nil }
func (s *importRegionSnapshotStreamFeed) SetTrailer(metadata.MD)       {}
func (s *importRegionSnapshotStreamFeed) Context() context.Context     { return s.ctx }
func (s *importRegionSnapshotStreamFeed) SendMsg(any) error            { return nil }
func (s *importRegionSnapshotStreamFeed) RecvMsg(any) error            { return nil }

func TestServiceImportRegionSnapshotStreamRejectsMissingHeader(t *testing.T) {
	svc := &Service{store: &store.Store{}, snapshot: noopSnapshotStore{}}
	stream := &importRegionSnapshotStreamFeed{
		ctx: context.Background(),
		reqs: []*pb.ImportRegionSnapshotStreamRequest{{
			Region: &pb.RegionMeta{Id: 1},
			Chunk:  []byte("payload"),
		}},
	}
	err := svc.ImportRegionSnapshotStream(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "snapshot_header is required")
}

func TestServiceImportRegionSnapshotStreamRejectsMissingRegion(t *testing.T) {
	svc := &Service{store: &store.Store{}, snapshot: noopSnapshotStore{}}
	header, err := (&raftpb.Snapshot{}).Marshal()
	require.NoError(t, err)
	stream := &importRegionSnapshotStreamFeed{
		ctx: context.Background(),
		reqs: []*pb.ImportRegionSnapshotStreamRequest{{
			SnapshotHeader: header,
			Chunk:          []byte("payload"),
		}},
	}
	err = svc.ImportRegionSnapshotStream(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "region is required")
}

func TestServiceImportRegionSnapshotStreamRejectsRepeatedHeader(t *testing.T) {
	svc := &Service{store: &store.Store{}, snapshot: noopSnapshotStore{}}
	header, err := (&raftpb.Snapshot{}).Marshal()
	require.NoError(t, err)
	stream := &importRegionSnapshotStreamFeed{
		ctx: context.Background(),
		reqs: []*pb.ImportRegionSnapshotStreamRequest{
			{
				SnapshotHeader: header,
				Region: &pb.RegionMeta{
					Id:       1,
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
				},
			},
			{
				SnapshotHeader: header,
				Chunk:          []byte("payload"),
			},
		},
	}
	err = svc.ImportRegionSnapshotStream(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "snapshot header may only appear in the first chunk")
}

func TestServiceExportsAndImportsRegionSnapshotStream(t *testing.T) {
	sourceDir := t.TempDir()
	sourceDB, sourceMeta := openAdminTestDBWithTweak(t, sourceDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	defer func() {
		require.NoError(t, sourceDB.Close())
		require.NoError(t, sourceMeta.Close())
	}()

	valueBacked := bytes.Repeat([]byte("z"), 4096)
	entry := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("alpha"), 9, valueBacked, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{entry}))

	region := raftmeta.RegionMeta{
		ID:       32,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    raftmeta.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []raftmeta.PeerMeta{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: raftmeta.RegionStateRunning,
	}
	require.NoError(t, sourceMeta.SaveRegion(region))

	sourceStorage, err := sourceDB.RaftLog().Open(region.ID, sourceMeta)
	require.NoError(t, err)
	require.NoError(t, sourceStorage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{101},
			},
		},
	}))

	sourceStore := store.NewStore(store.Config{
		StoreID:   1,
		LocalMeta: sourceMeta,
		PeerBuilder: func(meta raftmeta.RegionMeta) (*peer.Config, error) {
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              101,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:      noopTransport{},
				Apply:          func([]myraft.Entry) error { return nil },
				SnapshotExport: testSSTExport(sourceDB),
				Storage:        sourceStorage,
				GroupID:        meta.ID,
				Region:         raftmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer sourceStore.Close()

	sourcePeerCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:      noopTransport{},
		Apply:          func([]myraft.Entry) error { return nil },
		SnapshotExport: testSSTExport(sourceDB),
		Storage:        sourceStorage,
		GroupID:        region.ID,
		Region:         raftmeta.CloneRegionMetaPtr(&region),
	}
	_, err = sourceStore.StartPeer(sourcePeerCfg, nil)
	require.NoError(t, err)

	sourcePeer, ok := sourceStore.Peer(101)
	require.True(t, ok)
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool {
		status, ok := sourceStore.RegionRuntimeStatus(region.ID)
		return ok && status.Leader
	}, 2_000_000_000, 20_000_000)

	targetDir := t.TempDir()
	targetDB, targetMeta := openAdminTestDBWithTweak(t, targetDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	defer func() {
		require.NoError(t, targetDB.Close())
		require.NoError(t, targetMeta.Close())
	}()
	targetStore := store.NewStore(store.Config{
		StoreID:   2,
		LocalMeta: targetMeta,
		PeerBuilder: func(meta raftmeta.RegionMeta) (*peer.Config, error) {
			storage, err := targetDB.RaftLog().Open(meta.ID, targetMeta)
			require.NoError(t, err)
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              201,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:     noopTransport{},
				Apply:         func([]myraft.Entry) error { return nil },
				SnapshotApply: testSSTApply(targetDB),
				Storage:       storage,
				GroupID:       meta.ID,
				Region:        raftmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer targetStore.Close()

	sourceSvc := NewServiceWithSnapshot(sourceStore, sourceDB)
	targetSvc := NewServiceWithSnapshot(targetStore, targetDB)

	exportStream := &exportRegionSnapshotStreamCapture{ctx: context.Background()}
	require.NoError(t, sourceSvc.ExportRegionSnapshotStream(&pb.ExportRegionSnapshotStreamRequest{
		RegionId: region.ID,
	}, exportStream))
	require.NotEmpty(t, exportStream.chunks)

	importReqs := make([]*pb.ImportRegionSnapshotStreamRequest, 0, len(exportStream.chunks))
	for _, chunk := range exportStream.chunks {
		importReqs = append(importReqs, &pb.ImportRegionSnapshotStreamRequest{
			SnapshotHeader: append([]byte(nil), chunk.GetSnapshotHeader()...),
			Region:         chunk.GetRegion(),
			Chunk:          append([]byte(nil), chunk.GetChunk()...),
		})
	}
	importStream := &importRegionSnapshotStreamFeed{ctx: context.Background(), reqs: importReqs}
	require.NoError(t, targetSvc.ImportRegionSnapshotStream(importStream))
	require.NotNil(t, importStream.resp)
	require.Equal(t, region.ID, importStream.resp.GetRegion().GetId())

	status, ok := targetStore.RegionRuntimeStatus(region.ID)
	require.True(t, ok)
	require.True(t, status.Hosted)
	require.Equal(t, uint64(201), status.LocalPeerID)
	require.GreaterOrEqual(t, status.AppliedIndex, uint64(1))

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("alpha"), 9)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, valueBacked, got.Value)
}

func TestServiceImportRegionSnapshotStreamRejectsMismatchedRegionMeta(t *testing.T) {
	sourceDir := t.TempDir()
	sourceDB, sourceMeta := openAdminTestDBWithTweak(t, sourceDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	defer func() {
		require.NoError(t, sourceDB.Close())
		require.NoError(t, sourceMeta.Close())
	}()

	valueBacked := bytes.Repeat([]byte("z"), 4096)
	entry := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("alpha"), 9, valueBacked, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{entry}))

	region := raftmeta.RegionMeta{
		ID:       42,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    raftmeta.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []raftmeta.PeerMeta{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: raftmeta.RegionStateRunning,
	}
	require.NoError(t, sourceMeta.SaveRegion(region))

	sourceStorage, err := sourceDB.RaftLog().Open(region.ID, sourceMeta)
	require.NoError(t, err)
	require.NoError(t, sourceStorage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{101},
			},
		},
	}))

	sourceStore := store.NewStore(store.Config{
		StoreID:   1,
		LocalMeta: sourceMeta,
		PeerBuilder: func(meta raftmeta.RegionMeta) (*peer.Config, error) {
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              101,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:      noopTransport{},
				Apply:          func([]myraft.Entry) error { return nil },
				SnapshotExport: testSSTExport(sourceDB),
				Storage:        sourceStorage,
				GroupID:        meta.ID,
				Region:         raftmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer sourceStore.Close()

	sourcePeerCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:      noopTransport{},
		Apply:          func([]myraft.Entry) error { return nil },
		SnapshotExport: testSSTExport(sourceDB),
		Storage:        sourceStorage,
		GroupID:        region.ID,
		Region:         raftmeta.CloneRegionMetaPtr(&region),
	}
	_, err = sourceStore.StartPeer(sourcePeerCfg, nil)
	require.NoError(t, err)

	sourcePeer, ok := sourceStore.Peer(101)
	require.True(t, ok)
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool {
		status, ok := sourceStore.RegionRuntimeStatus(region.ID)
		return ok && status.Leader
	}, 2_000_000_000, 20_000_000)

	targetDir := t.TempDir()
	targetDB, targetMeta := openAdminTestDBWithTweak(t, targetDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	defer func() {
		require.NoError(t, targetDB.Close())
		require.NoError(t, targetMeta.Close())
	}()
	targetStore := store.NewStore(store.Config{
		StoreID:   2,
		LocalMeta: targetMeta,
		PeerBuilder: func(meta raftmeta.RegionMeta) (*peer.Config, error) {
			storage, err := targetDB.RaftLog().Open(meta.ID, targetMeta)
			require.NoError(t, err)
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              201,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:     noopTransport{},
				Apply:         func([]myraft.Entry) error { return nil },
				SnapshotApply: testSSTApply(targetDB),
				Storage:       storage,
				GroupID:       meta.ID,
				Region:        raftmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer targetStore.Close()

	sourceSvc := NewServiceWithSnapshot(sourceStore, sourceDB)
	targetSvc := NewServiceWithSnapshot(targetStore, targetDB)

	exportStream := &exportRegionSnapshotStreamCapture{ctx: context.Background()}
	require.NoError(t, sourceSvc.ExportRegionSnapshotStream(&pb.ExportRegionSnapshotStreamRequest{
		RegionId: region.ID,
	}, exportStream))
	require.NotEmpty(t, exportStream.chunks)

	importReqs := make([]*pb.ImportRegionSnapshotStreamRequest, 0, len(exportStream.chunks))
	for i, chunk := range exportStream.chunks {
		req := &pb.ImportRegionSnapshotStreamRequest{
			SnapshotHeader: append([]byte(nil), chunk.GetSnapshotHeader()...),
			Region:         chunk.GetRegion(),
			Chunk:          append([]byte(nil), chunk.GetChunk()...),
		}
		if i == 0 {
			req.Region = &pb.RegionMeta{
				Id:               chunk.GetRegion().GetId(),
				StartKey:         append([]byte(nil), chunk.GetRegion().GetStartKey()...),
				EndKey:           []byte("zz"),
				EpochVersion:     chunk.GetRegion().GetEpochVersion(),
				EpochConfVersion: chunk.GetRegion().GetEpochConfVersion(),
				Peers:            append([]*pb.RegionPeer(nil), chunk.GetRegion().GetPeers()...),
			}
		}
		importReqs = append(importReqs, req)
	}
	importStream := &importRegionSnapshotStreamFeed{ctx: context.Background(), reqs: importReqs}
	err = targetSvc.ImportRegionSnapshotStream(importStream)
	require.Error(t, err)
	require.ErrorContains(t, err, "region snapshot metadata mismatch")

	status, ok := targetStore.RegionRuntimeStatus(region.ID)
	require.False(t, ok && status.Hosted)

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("alpha"), 9)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.Nil(t, got)
}
