package controlplane

import (
	"context"
	"net"
	"testing"
	"time"

	coordablation "github.com/feichai0017/NoKV/coordinator/ablation"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootserver "github.com/feichai0017/NoKV/meta/root/server"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type witnessBenchmarkVariant struct {
	name     string
	ablation coordablation.Config
}

type witnessBenchmarkHarness struct {
	client   *coordclient.GRPCClient
	server   *grpc.Server
	listener *bufconn.Listener
}

func BenchmarkControlPlaneAllocIDWitnessTax(b *testing.B) {
	req := &coordpb.AllocIDRequest{Count: 1}
	for _, variant := range witnessBenchmarkVariants() {
		b.Run(variant.name, func(b *testing.B) {
			h := openWitnessBenchmarkHarness(b, variant.ablation)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := h.client.AllocID(ctx, req); err != nil {
					b.Fatalf("alloc id: %v", err)
				}
			}
		})
	}
}

func BenchmarkControlPlaneTSOWitnessTax(b *testing.B) {
	req := &coordpb.TsoRequest{Count: 1}
	for _, variant := range witnessBenchmarkVariants() {
		b.Run(variant.name, func(b *testing.B) {
			h := openWitnessBenchmarkHarness(b, variant.ablation)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := h.client.Tso(ctx, req); err != nil {
					b.Fatalf("tso: %v", err)
				}
			}
		})
	}
}

func BenchmarkControlPlaneMetadataWitnessTax(b *testing.B) {
	req := &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	}
	for _, variant := range witnessBenchmarkVariants() {
		b.Run(variant.name, func(b *testing.B) {
			h := openWitnessBenchmarkHarness(b, variant.ablation)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := h.client.GetRegionByKey(ctx, req)
				if err != nil {
					b.Fatalf("get region by key: %v", err)
				}
				if resp.GetNotFound() || resp.GetRegionDescriptor().GetRegionId() != 701 {
					b.Fatalf("unexpected metadata response: %+v", resp)
				}
			}
		})
	}
}

func witnessBenchmarkVariants() []witnessBenchmarkVariant {
	return []witnessBenchmarkVariant{
		{name: string(coordablation.PresetFull), ablation: mustWitnessPresetConfig(coordablation.PresetFull)},
		{name: string(coordablation.PresetClientBlind), ablation: mustWitnessPresetConfig(coordablation.PresetClientBlind)},
		{
			name:     string(coordablation.PresetReplyBlindClientBlind),
			ablation: mustWitnessPresetConfig(coordablation.PresetReplyBlindClientBlind),
		},
	}
}

func mustWitnessPresetConfig(preset coordablation.Preset) coordablation.Config {
	cfg, err := preset.Config()
	if err != nil {
		panic(err)
	}
	return cfg
}

func openWitnessBenchmarkHarness(b *testing.B, cfg coordablation.Config) *witnessBenchmarkHarness {
	b.Helper()

	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)

	cluster := catalog.NewCluster()
	desc := benchmarkRoutingDescriptor()
	desc.RootEpoch = 1
	if err := cluster.PublishRegionDescriptor(desc); err != nil {
		b.Fatalf("publish witness benchmark descriptor: %v", err)
	}

	svc := coordserver.NewService(cluster, idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	if err := svc.ConfigureAblation(cfg); err != nil {
		b.Fatalf("configure server ablation: %v", err)
	}

	server := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(server, svc)
	go func() {
		_ = server.Serve(listener)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := coordclient.NewGRPCClient(
		ctx,
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		b.Fatalf("new grpc client: %v", err)
	}
	if err := client.ConfigureAblation(cfg); err != nil {
		b.Fatalf("configure client ablation: %v", err)
	}

	h := &witnessBenchmarkHarness{
		client:   client,
		server:   server,
		listener: listener,
	}
	b.Cleanup(func() {
		_ = h.client.Close()
		h.server.GracefulStop()
		_ = h.listener.Close()
	})
	return h
}

func openBenchmarkRemoteRootServerTCP(tb testing.TB, backend rootserver.Backend) (string, func()) {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	server := grpc.NewServer()
	metapb.RegisterMetadataRootServer(server, rootserver.NewService(backend))
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.GracefulStop()
	}
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
