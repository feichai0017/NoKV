package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	"github.com/feichai0017/NoKV/pd/tso"
)

func TestNewGRPCClientEmptyAddress(t *testing.T) {
	cli, err := NewGRPCClient(context.Background(), "")
	require.Error(t, err)
	require.Nil(t, cli)
}

func TestGRPCClientRoundTrip(t *testing.T) {
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	svc := pdserver.NewService(core.NewCluster(), core.NewIDAllocator(10), tso.NewAllocator(100))
	grpcServer := grpc.NewServer()
	pb.RegisterPDServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.GracefulStop)

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewGRPCClient(ctx, "bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	storeResp, err := cli.StoreHeartbeat(context.Background(), &pb.StoreHeartbeatRequest{
		StoreId:   1,
		RegionNum: 2,
		LeaderNum: 1,
		Capacity:  1024,
		Available: 800,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())

	_, err = cli.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               11,
			StartKey:         []byte("a"),
			EndKey:           []byte("z"),
			EpochVersion:     1,
			EpochConfVersion: 1,
		},
	})
	require.NoError(t, err)

	getResp, err := cli.GetRegionByKey(context.Background(), &pb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, uint64(11), getResp.GetRegion().GetId())

	idResp, err := cli.AllocID(context.Background(), &pb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(2), idResp.GetCount())

	tsResp, err := cli.Tso(context.Background(), &pb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(3), tsResp.GetCount())
}
