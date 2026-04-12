package server

import (
	"context"
	"net"
	"testing"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func BenchmarkControlPlaneAllocIDLocalWindowDefault(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RemoteRoot: false,
		WindowSize: defaultAllocatorWindowSize,
	})
}

func BenchmarkControlPlaneAllocIDLocalWindowOne(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RemoteRoot: false,
		WindowSize: 1,
	})
}

func BenchmarkControlPlaneAllocIDRemoteWindowDefault(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RemoteRoot: true,
		WindowSize: defaultAllocatorWindowSize,
	})
}

func BenchmarkControlPlaneAllocIDRemoteWindowOne(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RemoteRoot: true,
		WindowSize: 1,
	})
}

type benchmarkControlPlaneConfig struct {
	RemoteRoot bool
	WindowSize uint64
}

func benchmarkControlPlaneAllocID(b *testing.B, cfg benchmarkControlPlaneConfig) {
	b.Helper()
	svc, cleanup := openBenchmarkCoordinatorService(b, cfg)
	defer cleanup()

	ctx := context.Background()
	req := &coordpb.AllocIDRequest{Count: 1}
	if _, err := svc.AllocID(ctx, req); err != nil {
		b.Fatalf("warmup alloc id: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := svc.AllocID(ctx, req); err != nil {
			b.Fatalf("alloc id: %v", err)
		}
	}
}

func openBenchmarkCoordinatorService(b *testing.B, cfg benchmarkControlPlaneConfig) (*Service, func()) {
	b.Helper()
	backend, err := rootlocal.Open(b.TempDir(), nil)
	require.NoError(b, err)

	var (
		store   *coordstorage.RootStore
		cleanup func()
	)
	if cfg.RemoteRoot {
		store, cleanup = openBenchmarkRemoteRootStore(b, backend)
	} else {
		store, err = coordstorage.OpenRootStore(backend)
		require.NoError(b, err)
		cleanup = func() { require.NoError(b, store.Close()) }
	}

	cluster := catalog.NewCluster()
	bootstrap, err := coordstorage.Bootstrap(store, cluster.PublishRegionDescriptor, 1, 1)
	require.NoError(b, err)
	svc := NewService(
		cluster,
		idalloc.NewIDAllocator(bootstrap.IDStart),
		tso.NewAllocator(bootstrap.TSStart),
		store,
	)
	svc.idWindowSize = cfg.WindowSize
	svc.tsoWindowSize = cfg.WindowSize
	return svc, cleanup
}

func openBenchmarkRemoteRootStore(b *testing.B, backend rootremote.Backend) (*coordstorage.RootStore, func()) {
	b.Helper()
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	metapb.RegisterMetadataRootServer(server, rootremote.NewService(backend))
	go func() { _ = server.Serve(listener) }()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	store, err := coordstorage.OpenRootRemoteStore(coordstorage.RemoteRootConfig{
		Targets: map[uint64]string{1: "passthrough:///bufnet"},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
		},
	})
	require.NoError(b, err)

	cleanup := func() {
		require.NoError(b, store.Close())
		server.GracefulStop()
		require.NoError(b, listener.Close())
	}
	return store, cleanup
}
