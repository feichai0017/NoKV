package controlplane

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/coordinator/tso"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func BenchmarkControlPlaneGetRegionByKeyRemoteTCPOneCoordinator(b *testing.B) {
	benchmarkSeparatedRoutingLookup(b, 1)
}

func BenchmarkControlPlaneGetRegionByKeyRemoteTCPThreeCoordinators(b *testing.B) {
	benchmarkSeparatedRoutingLookup(b, 3)
}

func benchmarkSeparatedRoutingLookup(b *testing.B, coordinatorCount int) {
	b.Helper()
	services, cleanup := openBenchmarkSeparatedRoutingServices(b, coordinatorCount)
	defer cleanup()

	ctx := context.Background()
	req := &coordpb.GetRegionByKeyRequest{
		Key:        []byte("m"),
		Freshness:  coordpb.Freshness_FRESHNESS_BEST_EFFORT,
		MaxRootLag: nil,
	}
	for _, svc := range services {
		resp, err := svc.GetRegionByKey(ctx, req)
		if err != nil {
			b.Fatalf("warmup get region by key: %v", err)
		}
		if resp.GetNotFound() || resp.GetRegionDescriptor().GetRegionId() != 701 {
			b.Fatalf("warmup returned unexpected descriptor: %+v", resp)
		}
	}

	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			svc := services[next.Add(1)%uint64(len(services))]
			resp, err := svc.GetRegionByKey(ctx, req)
			if err != nil {
				b.Fatalf("get region by key: %v", err)
			}
			if resp.GetNotFound() || resp.GetRegionDescriptor().GetRegionId() != 701 {
				b.Fatalf("unexpected descriptor: %+v", resp)
			}
		}
	})
}

func openBenchmarkSeparatedRoutingServices(b *testing.B, coordinatorCount int) ([]*coordserver.Service, func()) {
	b.Helper()
	if coordinatorCount < 1 {
		b.Fatalf("coordinatorCount must be >= 1")
	}

	backend, err := rootlocal.Open(b.TempDir(), nil)
	require.NoError(b, err)
	addr, stopRemote := openBenchmarkRemoteRootServerTCP(b, backend)

	seedStore, err := coordstorage.OpenRootStore(backend)
	require.NoError(b, err)
	_, err = coordstorage.Bootstrap(seedStore, func(descriptor.Descriptor) error { return nil }, 1, 1)
	require.NoError(b, err)
	require.NoError(b, seedStore.AppendRootEvent(rootevent.RegionBootstrapped(benchmarkRoutingDescriptor())))
	require.NoError(b, seedStore.Close())

	services := make([]*coordserver.Service, 0, coordinatorCount)
	stores := make([]*coordstorage.RootStore, 0, coordinatorCount)
	for i := 0; i < coordinatorCount; i++ {
		store, err := coordstorage.OpenRootRemoteStore(coordstorage.RemoteRootConfig{
			Targets: map[uint64]string{1: addr},
		})
		require.NoError(b, err)
		stores = append(stores, store)

		cluster := catalog.NewCluster()
		bootstrap, err := coordstorage.Bootstrap(store, cluster.PublishRegionDescriptor, 1, 1)
		require.NoError(b, err)
		services = append(services, coordserver.NewService(
			cluster,
			idalloc.NewIDAllocator(bootstrap.IDStart),
			tso.NewAllocator(bootstrap.TSStart),
			store,
		))
	}

	cleanup := func() {
		for _, store := range stores {
			require.NoError(b, store.Close())
		}
		stopRemote()
	}
	return services, cleanup
}

func benchmarkRoutingDescriptor() descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: 701,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
