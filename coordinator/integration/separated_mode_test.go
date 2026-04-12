package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSeparatedModeCoordinatorCrashAndRecoveryPreservesAllocatorFence(t *testing.T) {
	rootCluster := pdtestcluster.OpenReplicated(t)
	targets := exposeRemoteRoots(t, rootCluster)
	rootCluster.WaitLeader()

	first, firstStore := openSeparatedCoordinator(t, targets, "c1")
	alloc, err := first.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 4})
	require.NoError(t, err)
	lastID := alloc.GetFirstId() + alloc.GetCount() - 1
	require.NoError(t, firstStore.Close())

	leaderID := rootCluster.WaitLeader()
	require.Eventually(t, func() bool {
		state, err := rootCluster.Roots[leaderID].Current()
		return err == nil && state.IDFence >= lastID
	}, 8*time.Second, 50*time.Millisecond)

	recovered, recoveredStore := openSeparatedCoordinator(t, targets, "c1")
	t.Cleanup(func() { require.NoError(t, recoveredStore.Close()) })

	next, err := recovered.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Greater(t, next.GetFirstId(), lastID)

	require.Eventually(t, func() bool {
		state, err := rootCluster.Roots[rootCluster.WaitLeader()].Current()
		if err != nil {
			return false
		}
		return state.IDFence >= next.GetFirstId() &&
			state.CoordinatorLease.HolderID == "c1" &&
			state.CoordinatorLease.IDFence >= lastID
	}, 8*time.Second, 50*time.Millisecond)
}

func exposeRemoteRoots(t *testing.T, cluster *pdtestcluster.Cluster) map[uint64]string {
	t.Helper()
	targets := make(map[uint64]string, len(cluster.Roots))
	for id, root := range cluster.Roots {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		server := grpc.NewServer()
		rootremote.Register(server, root)
		go func() { _ = server.Serve(lis) }()
		t.Cleanup(server.GracefulStop)
		t.Cleanup(func() { require.NoError(t, lis.Close()) })
		targets[id] = lis.Addr().String()
	}
	return targets
}

func openSeparatedCoordinator(t *testing.T, targets map[uint64]string, coordinatorID string) (*coordserver.Service, *coordstorage.RootStore) {
	t.Helper()
	store, err := coordstorage.OpenRootRemoteStore(coordstorage.RemoteRootConfig{
		Targets: targets,
	})
	require.NoError(t, err)

	cluster := catalog.NewCluster()
	bootstrap, err := coordstorage.Bootstrap(store, cluster.PublishRegionDescriptor, 1, 1)
	require.NoError(t, err)

	svc := coordserver.NewService(
		cluster,
		idalloc.NewIDAllocator(bootstrap.IDStart),
		tso.NewAllocator(bootstrap.TSStart),
		store,
	)
	svc.ConfigureCoordinatorLease(coordinatorID, 10*time.Second, 3*time.Second)
	return svc, store
}
