package client

import (
	"context"
	"errors"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"net"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	"github.com/feichai0017/NoKV/pd/tso"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	pdpb.RegisterPDServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.GracefulStop)

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewGRPCClient(ctx, "passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	storeResp, err := cli.StoreHeartbeat(context.Background(), &pdpb.StoreHeartbeatRequest{
		StoreId:   1,
		RegionNum: 2,
		LeaderNum: 1,
		Capacity:  1024,
		Available: 800,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())

	_, err = cli.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: metacodec.DescriptorToProto(testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		})),
	})
	require.NoError(t, err)

	_, err = cli.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdded(
			11,
			2,
			201,
			testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{
				Version:     1,
				ConfVersion: 2,
			}),
		)),
	})
	require.NoError(t, err)

	getResp, err := cli.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, uint64(11), getResp.GetRegionDescriptor().GetRegionId())

	removeResp, err := cli.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, removeResp.GetRemoved())

	getResp, err = cli.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	idResp, err := cli.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(2), idResp.GetCount())

	tsResp, err := cli.Tso(context.Background(), &pdpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(3), tsResp.GetCount())
}

func TestGRPCClientWriteFailoverAcrossPDs(t *testing.T) {
	const bufSize = 1 << 20
	followerListener := bufconn.Listen(bufSize)
	leaderListener := bufconn.Listen(bufSize)
	t.Cleanup(func() {
		_ = followerListener.Close()
		_ = leaderListener.Close()
	})

	followerSvc := pdserver.NewService(core.NewCluster(), core.NewIDAllocator(10), tso.NewAllocator(100))
	followerSvc.SetStorage(&followerStorage{})
	followerGRPC := grpc.NewServer()
	pdpb.RegisterPDServer(followerGRPC, followerSvc)
	go func() { _ = followerGRPC.Serve(followerListener) }()
	t.Cleanup(followerGRPC.GracefulStop)

	leaderSvc := pdserver.NewService(core.NewCluster(), core.NewIDAllocator(10), tso.NewAllocator(100))
	leaderGRPC := grpc.NewServer()
	pdpb.RegisterPDServer(leaderGRPC, leaderSvc)
	go func() { _ = leaderGRPC.Serve(leaderListener) }()
	t.Cleanup(leaderGRPC.GracefulStop)

	dialer := func(_ context.Context, target string) (net.Conn, error) {
		switch target {
		case "bufnet-follower":
			return followerListener.Dial()
		case "bufnet-leader":
			return leaderListener.Dial()
		default:
			return nil, errors.New("unknown target: " + target)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewGRPCClient(ctx, "passthrough:///bufnet-follower,passthrough:///bufnet-leader",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	idResp, err := cli.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(2), idResp.GetCount())

	tsResp, err := cli.Tso(context.Background(), &pdpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
}

type followerStorage struct{}

func (f *followerStorage) Load() (pdstorage.Snapshot, error) {
	return pdstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
}
func (f *followerStorage) PublishRegionDescriptor(descriptor.Descriptor) error { return nil }
func (f *followerStorage) AppendRootEvent(rootevent.Event) error               { return nil }
func (f *followerStorage) TombstoneRegion(uint64) error                        { return nil }
func (f *followerStorage) SaveAllocatorState(uint64, uint64) error             { return nil }
func (f *followerStorage) Refresh() error                                      { return nil }
func (f *followerStorage) Close() error                                        { return nil }
func (f *followerStorage) IsLeader() bool                                      { return false }
func (f *followerStorage) LeaderID() uint64                                    { return 2 }

func TestGRPCClientDoesNotRetryReadOnNotLeaderWriteError(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, errNotLeaderPrefix+" (leader_id=2)")
	require.True(t, retryableWrite(err))
	require.False(t, retryableRead(err))
}

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     epoch,
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
