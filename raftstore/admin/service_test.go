package admin

import (
	"context"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type noopTransport struct{}

func (noopTransport) Send(context.Context, myraft.Message) {}

func openAdminTestDB(t *testing.T, dir string) (*NoKV.DB, *raftmeta.Store) {
	t.Helper()
	localMeta, err := raftmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db, localMeta
}

func TestServiceExportsAndInstallsRegionSnapshot(t *testing.T) {
	sourceDir := t.TempDir()
	sourceDB, sourceMeta := openAdminTestDB(t, sourceDir)
	defer func() {
		require.NoError(t, sourceDB.Close())
		require.NoError(t, sourceMeta.Close())
	}()

	entry := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("alpha"), 9, []byte("value"), 0, 0)
	defer entry.DecrRef()
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{entry}))

	region := raftmeta.RegionMeta{
		ID:       12,
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
				Transport: noopTransport{},
				Apply:     func([]myraft.Entry) error { return nil },
				SnapshotExport: func(region raftmeta.RegionMeta) ([]byte, error) {
					payload, _, err := snapshotpkg.ExportPayload(sourceDB, region)
					return payload, err
				},
				Storage: sourceStorage,
				GroupID: meta.ID,
				Region:  raftmeta.CloneRegionMetaPtr(&meta),
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
			Transport: noopTransport{},
			Apply:     func([]myraft.Entry) error { return nil },
			SnapshotExport: func(region raftmeta.RegionMeta) ([]byte, error) {
				payload, _, err := snapshotpkg.ExportPayload(sourceDB, region)
				return payload, err
			},
			Storage: sourceStorage,
			GroupID: region.ID,
			Region:  raftmeta.CloneRegionMetaPtr(&region),
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
	targetDB, targetMeta := openAdminTestDB(t, targetDir)
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
				Transport: noopTransport{},
				Apply:     func([]myraft.Entry) error { return nil },
				SnapshotApply: func(payload []byte) (raftmeta.RegionMeta, error) {
					result, err := snapshotpkg.ImportPayload(targetDB, payload)
					if err != nil {
						return raftmeta.RegionMeta{}, err
					}
					return result.Manifest.Region, nil
				},
				Storage: storage,
				GroupID: meta.ID,
				Region:  raftmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer targetStore.Close()

	sourceSvc := NewService(sourceStore)
	targetSvc := NewService(targetStore)

	exported, err := sourceSvc.ExportRegionSnapshot(context.Background(), &pb.ExportRegionSnapshotRequest{RegionId: region.ID})
	require.NoError(t, err)
	require.NotEmpty(t, exported.GetSnapshot())

	installed, err := targetSvc.InstallRegionSnapshot(context.Background(), &pb.InstallRegionSnapshotRequest{Snapshot: exported.GetSnapshot()})
	require.NoError(t, err)
	require.Equal(t, region.ID, installed.GetRegion().GetId())

	status, ok := targetStore.RegionRuntimeStatus(region.ID)
	require.True(t, ok)
	require.True(t, status.Hosted)
	require.Equal(t, uint64(201), status.LocalPeerID)
	require.Equal(t, uint64(2), status.AppliedIndex)

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("alpha"), 9)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, []byte("value"), got.Value)
}
