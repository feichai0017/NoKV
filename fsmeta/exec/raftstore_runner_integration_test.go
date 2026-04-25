package exec

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/client"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRaftstoreRunnerExecutorContractOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint-0001",
		Inode:  42,
	}
	err := executor.Create(ctx, req, fsmeta.InodeRecord{
		Type:      fsmeta.InodeTypeFile,
		Size:      4096,
		LinkCount: 1,
		Mode:      0o644,
	})
	require.NoError(t, err)

	record, err := executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	entries, err := executor.ReadDir(ctx, fsmeta.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryRecord{{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   fsmeta.InodeTypeFile,
	}}, entries)

	pairs, err := executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{
			Parent: req.Parent,
			Name:   req.Name,
			Inode:  req.Inode,
			Type:   fsmeta.InodeTypeFile,
		},
		Inode: fsmeta.InodeRecord{
			Inode:     req.Inode,
			Type:      fsmeta.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}}, pairs)

	err = executor.Create(ctx, req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.True(t, errors.Is(err, fsmeta.ErrExists), "duplicate create error = %v", err)
}

func TestRaftstoreRunnerRenameAcrossRegionsOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	executor := openSplitRealClusterExecutor(t, ctx)

	err := executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
		Inode:  61,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	err = executor.Rename(ctx, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   "alpha",
		ToParent:   fsmeta.RootInode,
		ToName:     "zulu",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	record, err := executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "zulu",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "zulu",
		Inode:  61,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	require.NoError(t, executor.Unlink(ctx, fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "zulu",
	}))
	_, err = executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "zulu",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

func openRealClusterExecutor(t *testing.T, ctx context.Context) *Executor {
	t.Helper()

	coord := testcluster.StartCoordinator(t)
	t.Cleanup(func() { coord.Close(t) })

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	const (
		storeID  = uint64(1)
		regionID = uint64(121)
		peerID   = uint64(101)
	)
	_, err := migrate.Init(migrate.InitConfig{
		WorkDir:  seedDir,
		StoreID:  storeID,
		RegionID: regionID,
		PeerID:   peerID,
	})
	require.NoError(t, err)

	node := testcluster.StartNodeWithConfig(t, storeID, seedDir, testcluster.NodeConfig{
		AllowedModes:      []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster},
		StartPeers:        true,
		Scheduler:         testcluster.NewScheduler(t, coord.Addr(), 100*time.Millisecond),
		HeartbeatInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { node.Close(t) })

	testcluster.WaitForLeaderPeer(t, ctx, node.Addr(), regionID, peerID)
	testcluster.WaitForSchedulerMode(t, node, storepkg.SchedulerModeHealthy, false)

	coordRPC, err := coordclient.NewGRPCClient(ctx, coord.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = coordRPC.Close() })
	kv, err := client.New(client.Config{
		StoreResolver:  coordRPC,
		RegionResolver: coordRPC,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	runner, err := NewRaftstoreRunner(kv, coordRPC)
	require.NoError(t, err)
	executor, err := New(runner)
	require.NoError(t, err)
	return executor
}

func openSplitRealClusterExecutor(t *testing.T, ctx context.Context) *Executor {
	t.Helper()

	coord := testcluster.StartCoordinator(t)
	t.Cleanup(func() { coord.Close(t) })

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	const (
		storeID        = uint64(1)
		parentRegionID = uint64(131)
		childRegionID  = uint64(132)
		parentPeerID   = uint64(101)
		childPeerID    = uint64(102)
	)
	_, err := migrate.Init(migrate.InitConfig{
		WorkDir:  seedDir,
		StoreID:  storeID,
		RegionID: parentRegionID,
		PeerID:   parentPeerID,
	})
	require.NoError(t, err)

	node := testcluster.StartNodeWithConfig(t, storeID, seedDir, testcluster.NodeConfig{
		AllowedModes:      []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster},
		StartPeers:        true,
		Scheduler:         testcluster.NewScheduler(t, coord.Addr(), 100*time.Millisecond),
		HeartbeatInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { node.Close(t) })

	testcluster.WaitForLeaderPeer(t, ctx, node.Addr(), parentRegionID, parentPeerID)
	testcluster.WaitForSchedulerMode(t, node, storepkg.SchedulerModeHealthy, false)

	splitKey, err := fsmeta.EncodeDentryKey("vol", fsmeta.RootInode, "m")
	require.NoError(t, err)
	childMeta := localmeta.RegionMeta{
		ID:       childRegionID,
		StartKey: splitKey,
		EndKey:   nil,
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []metaregion.Peer{{StoreID: storeID, PeerID: childPeerID}},
	}
	require.NoError(t, node.Server.Store().ProposeSplit(parentRegionID, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		status := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), childRegionID)
		return status.GetKnown() && status.GetHosted()
	}, 5*time.Second, 20*time.Millisecond)
	testcluster.WaitForLeaderPeer(t, ctx, node.Addr(), childRegionID, childPeerID)
	require.Eventually(t, func() bool {
		parent := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), parentRegionID)
		child := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), childRegionID)
		return bytes.Equal(parent.GetRegion().GetEndKey(), splitKey) &&
			bytes.Equal(child.GetRegion().GetStartKey(), splitKey)
	}, 5*time.Second, 20*time.Millisecond)

	parentStatus := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), parentRegionID)
	childStatus := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), childRegionID)
	coordRPC, err := coordclient.NewGRPCClient(ctx, coord.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	kv, err := client.New(client.Config{
		StoreResolver: coordRPC,
		RegionResolver: &staticRegionResolver{regions: []*metapb.RegionDescriptor{
			parentStatus.GetRegion(),
			childStatus.GetRegion(),
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	runner, err := NewRaftstoreRunner(kv, coordRPC)
	require.NoError(t, err)
	executor, err := New(runner)
	require.NoError(t, err)
	return executor
}

type staticRegionResolver struct {
	regions []*metapb.RegionDescriptor
}

func (r *staticRegionResolver) GetRegionByKey(_ context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	for _, region := range r.regions {
		if containsRegionKey(region, req.GetKey()) {
			return &coordpb.GetRegionByKeyResponse{
				RegionDescriptor: metawire.DescriptorToProto(metawire.DescriptorFromProto(region)),
			}, nil
		}
	}
	return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
}

func (r *staticRegionResolver) Close() error { return nil }

func containsRegionKey(region *metapb.RegionDescriptor, key []byte) bool {
	if region == nil {
		return false
	}
	if len(region.GetStartKey()) > 0 && bytes.Compare(key, region.GetStartKey()) < 0 {
		return false
	}
	if len(region.GetEndKey()) > 0 && bytes.Compare(key, region.GetEndKey()) >= 0 {
		return false
	}
	return true
}
