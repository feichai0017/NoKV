package integration_test

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
	"github.com/feichai0017/NoKV/coordinator/tso"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
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

	slices.Sort(durations)
	p50 := percentileDuration(durations, 0.50)
	p95 := percentileDuration(durations, 0.95)
	p99 := percentileDuration(durations, 0.99)
	avg := averageDuration(durations)

	t.Logf("separated coordinator recovery latency: n=%d avg=%s p50=%s p95=%s p99=%s",
		len(durations), avg, p50, p95, p99)
}

func TestSeparatedModeRoutingServesAcrossMultipleCoordinatorsWhileAllocatorStaysSingleton(t *testing.T) {
	rootCluster := pdtestcluster.OpenReplicated(t)
	targets := exposeRemoteRoots(t, rootCluster)
	rootCluster.WaitLeader()

	owner, ownerStore := openSeparatedCoordinator(t, targets, "c1")
	t.Cleanup(func() { require.NoError(t, ownerStore.Close()) })
	readOnlyA, readOnlyAStore := openSeparatedCoordinator(t, targets, "c2")
	t.Cleanup(func() { require.NoError(t, readOnlyAStore.Close()) })
	readOnlyB, readOnlyBStore := openSeparatedCoordinator(t, targets, "c3")
	t.Cleanup(func() { require.NoError(t, readOnlyBStore.Close()) })

	_, err := owner.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	_, err = owner.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(separatedModeDescriptor(731, []byte("a"), []byte("z")))),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, getErr := owner.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
		return getErr == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 731
	}, 8*time.Second, 50*time.Millisecond)

	for _, svc := range []*coordserver.Service{readOnlyA, readOnlyB} {
		require.Eventually(t, func() bool {
			if err := svc.RefreshFromStorage(); err != nil {
				return false
			}
			resp, getErr := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
			return getErr == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 731
		}, 8*time.Second, 50*time.Millisecond)
	}

	_, err = readOnlyA.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "coordinator lease not held")

	_, err = readOnlyB.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "coordinator lease not held")
}

func separatedModeDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}

func TestSeparatedModeCoordinatorContestedFailoverPreservesAllocatorFence(t *testing.T) {
	rootCluster := pdtestcluster.OpenReplicated(t)
	targets := exposeRemoteRoots(t, rootCluster)
	rootCluster.WaitLeader()

	leaseTTL := time.Second
	renewIn := 200 * time.Millisecond

	first, firstStore := openSeparatedCoordinatorWithLease(t, targets, "c1", leaseTTL, renewIn)
	alloc, err := first.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 8})
	require.NoError(t, err)
	lastID := alloc.GetFirstId() + alloc.GetCount() - 1
	require.NoError(t, firstStore.Close())

	second, secondStore := openSeparatedCoordinatorWithLease(t, targets, "c2", leaseTTL, renewIn)
	t.Cleanup(func() { require.NoError(t, secondStore.Close()) })

	_, err = second.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "coordinator lease not held")

	var next *coordpb.AllocIDResponse
	require.Eventually(t, func() bool {
		resp, allocErr := second.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
		if allocErr != nil {
			return false
		}
		next = resp
		return true
	}, 3*time.Second, 20*time.Millisecond)
	require.NotNil(t, next)
	require.Greater(t, next.GetFirstId(), lastID)

	require.Eventually(t, func() bool {
		state, currentErr := rootCluster.Roots[rootCluster.WaitLeader()].Current()
		if currentErr != nil {
			return false
		}
		return state.CoordinatorLease.HolderID == "c2" &&
			state.CoordinatorLease.IDFence >= lastID &&
			state.IDFence >= next.GetFirstId()
	}, 8*time.Second, 50*time.Millisecond)
}

func TestSeparatedModeCoordinatorChaosMonotonicAllocID(t *testing.T) {
	rootCluster := pdtestcluster.OpenReplicated(t)
	targets := exposeRemoteRoots(t, rootCluster)
	rootCluster.WaitLeader()

	iterations := 24
	batch := uint64(256)
	if testing.Short() {
		iterations = 8
		batch = 64
	}

	var (
		lastID        uint64
		recoveryGaps  []time.Duration
		firstIssuedID uint64
	)

	for i := 0; i < iterations; i++ {
		prevLastID := lastID
		svc, store := openSeparatedCoordinator(t, targets, "c1")

		start := time.Now()
		alloc, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: batch})
		require.NoError(t, err)
		recoveryGaps = append(recoveryGaps, time.Since(start))

		firstID := alloc.GetFirstId()
		lastIssued := firstID + alloc.GetCount() - 1
		if i == 0 {
			firstIssuedID = firstID
		} else {
			require.Greater(t, firstID, lastID, "iteration %d restarted below previous allocator high watermark", i)
		}
		require.GreaterOrEqual(t, lastIssued, firstID, "iteration %d returned invalid allocation range", i)
		lastID = lastIssued

		require.NoError(t, store.Close(), "iteration %d close failed", i)

		leaderID := rootCluster.WaitLeader()
		require.Eventually(t, func() bool {
			state, err := rootCluster.Roots[leaderID].Current()
			return err == nil &&
				state.IDFence >= lastID
		}, 8*time.Second, 50*time.Millisecond, "iteration %d rooted allocator state did not retain monotonic fence", i)

		if i > 0 {
			require.Eventually(t, func() bool {
				state, err := rootCluster.Roots[leaderID].Current()
				return err == nil &&
					state.CoordinatorLease.HolderID == "c1" &&
					state.CoordinatorLease.IDFence >= prevLastID
			}, 8*time.Second, 50*time.Millisecond, "iteration %d lease campaign did not inherit previous allocator fence", i)
		}
	}

	slices.Sort(recoveryGaps)
	t.Logf("separated coordinator chaos alloc monotonicity: iterations=%d batch=%d first=%d last=%d avg_recovery=%s p50=%s p95=%s",
		iterations,
		batch,
		firstIssuedID,
		lastID,
		averageDuration(recoveryGaps),
		percentileDuration(recoveryGaps, 0.50),
		percentileDuration(recoveryGaps, 0.95),
	)
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
	return openSeparatedCoordinatorWithLease(t, targets, coordinatorID, 10*time.Second, 3*time.Second)
}

func openSeparatedCoordinatorWithLease(t *testing.T, targets map[uint64]string, coordinatorID string, leaseTTL, renewIn time.Duration) (*coordserver.Service, *coordstorage.RootStore) {
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
	svc.ConfigureCoordinatorLease(coordinatorID, leaseTTL, renewIn)
	return svc, store
}
