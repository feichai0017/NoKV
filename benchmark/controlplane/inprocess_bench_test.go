package controlplane

import (
	"context"
	"net"
	"testing"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
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

const inProcessAllocatorWindowDefault uint64 = 10_000

func BenchmarkControlPlaneAllocIDLocalWindowDefault(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RootMode:   benchmarkRootModeLocal,
		WindowSize: inProcessAllocatorWindowDefault,
	})
}

func BenchmarkControlPlaneAllocIDLocalWindowOne(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RootMode:   benchmarkRootModeLocal,
		WindowSize: 1,
	})
}

func BenchmarkControlPlaneAllocIDRemoteWindowDefault(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RootMode:   benchmarkRootModeRemoteBufconn,
		WindowSize: inProcessAllocatorWindowDefault,
	})
}

func BenchmarkControlPlaneAllocIDRemoteWindowOne(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RootMode:   benchmarkRootModeRemoteBufconn,
		WindowSize: 1,
	})
}

func BenchmarkControlPlaneAllocIDRemoteTCPWindowDefault(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RootMode:   benchmarkRootModeRemoteTCP,
		WindowSize: inProcessAllocatorWindowDefault,
	})
}

func BenchmarkControlPlaneAllocIDRemoteTCPWindowOne(b *testing.B) {
	benchmarkControlPlaneAllocID(b, benchmarkControlPlaneConfig{
		RootMode:   benchmarkRootModeRemoteTCP,
		WindowSize: 1,
	})
}

type benchmarkControlPlaneConfig struct {
	RootMode   benchmarkRootMode
	WindowSize uint64
}

type benchmarkRootMode uint8

const (
	benchmarkRootModeLocal benchmarkRootMode = iota
	benchmarkRootModeRemoteBufconn
	benchmarkRootModeRemoteTCP
)

func benchmarkControlPlaneAllocID(b *testing.B, cfg benchmarkControlPlaneConfig) {
	b.Helper()
	svc, cleanup := openBenchmarkCoordinatorService(b, cfg)

	ctx := context.Background()
	req := &coordpb.AllocIDRequest{Count: 1}
	if _, err := svc.AllocID(ctx, req); err != nil {
		cleanup()
		b.Fatalf("warmup alloc id: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := svc.AllocID(ctx, req); err != nil {
			b.StopTimer()
			cleanup()
			b.Fatalf("alloc id: %v", err)
		}
	}
	b.StopTimer()
	cleanup()
}

func openBenchmarkCoordinatorService(b *testing.B, cfg benchmarkControlPlaneConfig) (*coordserver.Service, func()) {
	b.Helper()
	backend, err := rootlocal.Open(b.TempDir(), nil)
	require.NoError(b, err)

	var (
		store   *coordstorage.RootStore
		cleanup func()
	)
	switch cfg.RootMode {
	case benchmarkRootModeLocal:
		store, err = coordstorage.OpenRootStore(backend)
		require.NoError(b, err)
		cleanup = func() { require.NoError(b, store.Close()) }
	case benchmarkRootModeRemoteBufconn:
		store, cleanup = openBenchmarkRemoteRootStoreBufconn(b, backend)
	case benchmarkRootModeRemoteTCP:
		store, cleanup = openBenchmarkRemoteRootStoreTCP(b, backend)
	default:
		b.Fatalf("unknown benchmark root mode: %d", cfg.RootMode)
	}

	cluster := catalog.NewCluster()
	bootstrap, err := coordstorage.Bootstrap(store, cluster.PublishRegionDescriptor, 1, 1)
	require.NoError(b, err)
	svc := coordserver.NewService(
		cluster,
		idalloc.NewIDAllocator(bootstrap.IDStart),
		tso.NewAllocator(bootstrap.TSStart),
		store,
	)
	svc.ConfigureAllocatorWindows(cfg.WindowSize, cfg.WindowSize)
	return svc, cleanup
}

func openBenchmarkRemoteRootStoreBufconn(b *testing.B, backend rootremote.Backend) (*coordstorage.RootStore, func()) {
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
	}
	return store, cleanup
}

func openBenchmarkRemoteRootStoreTCP(b *testing.B, backend rootremote.Backend) (*coordstorage.RootStore, func()) {
	b.Helper()
	addr, stop := openBenchmarkRemoteRootServerTCP(b, backend)
	store, err := coordstorage.OpenRootRemoteStore(coordstorage.RemoteRootConfig{
		Targets: map[uint64]string{1: addr},
	})
	require.NoError(b, err)

	cleanup := func() {
		require.NoError(b, store.Close())
		stop()
	}
	return store, cleanup
}

func openBenchmarkRemoteRootServerTCP(b *testing.B, backend rootremote.Backend) (string, func()) {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	server := grpc.NewServer()
	metapb.RegisterMetadataRootServer(server, rootremote.NewService(backend))
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.GracefulStop()
	}
}
