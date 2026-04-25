package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
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

func openRealClusterExecutor(t *testing.T, ctx context.Context) *fsmetaexec.Executor {
	t.Helper()
	return openRealClusterRuntime(t, ctx).executor
}

type realClusterRuntime struct {
	executor *fsmetaexec.Executor
	node     *testcluster.Node
}

func openRealClusterRuntime(t *testing.T, ctx context.Context) *realClusterRuntime {
	t.Helper()
	coord := testcluster.StartCoordinator(t)
	t.Cleanup(func() { coord.Close(t) })

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	const (
		storeID  = uint64(1)
		regionID = uint64(171)
		peerID   = uint64(101)
	)
	coord.JoinStore(t, storeID)
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
	registerMount(t, ctx, coordRPC, "vol")
	kv, err := client.New(client.Config{
		StoreResolver:  coordRPC,
		RegionResolver: coordRPC,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	runner, err := fsmetaexec.NewRaftstoreRunner(kv, coordRPC)
	require.NoError(t, err)
	executor, err := fsmetaexec.New(runner, fsmetaexec.WithMountResolver(testMountResolver{coord: coordRPC}))
	require.NoError(t, err)
	return &realClusterRuntime{executor: executor, node: node}
}

func openSplitRealClusterExecutor(t *testing.T, ctx context.Context) *fsmetaexec.Executor {
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
	coord.JoinStore(t, storeID)
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
	t.Cleanup(func() { _ = coordRPC.Close() })
	registerMount(t, ctx, coordRPC, "vol")
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

	runner, err := fsmetaexec.NewRaftstoreRunner(kv, coordRPC)
	require.NoError(t, err)
	executor, err := fsmetaexec.New(runner, fsmetaexec.WithMountResolver(testMountResolver{coord: coordRPC}))
	require.NoError(t, err)
	return executor
}

type testMountResolver struct {
	coord *coordclient.GRPCClient
}

func (r testMountResolver) ResolveMount(ctx context.Context, mount fsmeta.MountID) (fsmetaexec.MountAdmission, error) {
	resp, err := r.coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return fsmetaexec.MountAdmission{}, err
	}
	if resp == nil || resp.GetNotFound() || resp.GetMount() == nil {
		return fsmetaexec.MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	info := resp.GetMount()
	return fsmetaexec.MountAdmission{
		MountID:       fsmeta.MountID(info.GetMountId()),
		RootInode:     fsmeta.InodeID(info.GetRootInode()),
		SchemaVersion: info.GetSchemaVersion(),
		Retired:       info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED,
	}, nil
}

func registerMount(t *testing.T, ctx context.Context, coord *coordclient.GRPCClient, mount fsmeta.MountID) {
	t.Helper()
	resp, err := coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.MountRegistered(string(mount), uint64(fsmeta.RootInode), 1)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
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
