package integration_test

import (
	"context"
	"net"
	"sort"
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

func TestSeparatedModeCoordinatorRecoveryLatency(t *testing.T) {
	rootCluster := pdtestcluster.OpenReplicated(t)
	targets := exposeRemoteRoots(t, rootCluster)
	rootCluster.WaitLeader()

	iterations := 8
	if testing.Short() {
		iterations = 3
	}
	durations := make([]time.Duration, 0, iterations)

	var lastID uint64
	for i := 0; i < iterations; i++ {
		oldSvc, oldStore := openSeparatedCoordinator(t, targets, "c1")
		alloc, err := oldSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
		require.NoError(t, err)
		if alloc.GetFirstId() <= lastID {
			t.Fatalf("expected alloc id to advance, got %d after %d", alloc.GetFirstId(), lastID)
		}
		lastID = alloc.GetFirstId()
		require.NoError(t, oldStore.Close())

		start := time.Now()
		newSvc, newStore := openSeparatedCoordinator(t, targets, "c1")
		alloc, err = newSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
		require.NoError(t, err)
		elapsed := time.Since(start)
		require.NoError(t, newStore.Close())

		if alloc.GetFirstId() <= lastID {
			t.Fatalf("expected recovery alloc id to advance, got %d after %d", alloc.GetFirstId(), lastID)
		}
		lastID = alloc.GetFirstId()
		durations = append(durations, elapsed)
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p50 := percentileDuration(durations, 0.50)
	p95 := percentileDuration(durations, 0.95)
	p99 := percentileDuration(durations, 0.99)
	avg := averageDuration(durations)

	t.Logf("separated coordinator recovery latency: n=%d avg=%s p50=%s p95=%s p99=%s",
		len(durations), avg, p50, p95, p99)
}

func percentileDuration(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if p <= 0 {
		return samples[0]
	}
	if p >= 1 {
		return samples[len(samples)-1]
	}
	idx := int(float64(len(samples)-1) * p)
	return samples[idx]
}

func averageDuration(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range samples {
		total += d
	}
	return total / time.Duration(len(samples))
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
