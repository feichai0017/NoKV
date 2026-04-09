package client

import (
	"context"
	"errors"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"net"
	"testing"
	"time"

	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
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

	svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	grpcServer := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(grpcServer, svc)
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

	storeResp, err := cli.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   1,
		RegionNum: 2,
		LeaderNum: 1,
		Capacity:  1024,
		Available: 800,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())

	publishResp, err := cli.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionBootstrapped(
			testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{
				Version:     1,
				ConfVersion: 1,
			}),
		)),
	})
	require.NoError(t, err)
	require.True(t, publishResp.GetAccepted())
	require.NotNil(t, publishResp.GetAssessment())

	liveResp, err := cli.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, liveResp.GetAccepted())

	publishResp, err = cli.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
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
	require.True(t, publishResp.GetAccepted())
	require.NotNil(t, publishResp.GetAssessment())
	require.Equal(t, "peer:11:2:201", publishResp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionPhase_TRANSITION_PHASE_PLANNED, publishResp.GetAssessment().GetPhase())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, publishResp.GetAssessment().GetDecision())

	getResp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, uint64(11), getResp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, getResp.GetServedFreshness())
	require.True(t, getResp.GetServedByLeader())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_HEALTHY, getResp.GetDegradedMode())

	removeResp, err := cli.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, removeResp.GetRemoved())

	getResp, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	idResp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(2), idResp.GetCount())

	tsResp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
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

	followerSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	followerSvc.SetStorage(&followerStorage{})
	followerGRPC := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(followerGRPC, followerSvc)
	go func() { _ = followerGRPC.Serve(followerListener) }()
	t.Cleanup(followerGRPC.GracefulStop)

	leaderSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	leaderGRPC := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(leaderGRPC, leaderSvc)
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

	idResp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(2), idResp.GetCount())

	tsResp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
}

type followerStorage struct{}

func (f *followerStorage) Load() (coordstorage.Snapshot, error) {
	return coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
}
func (f *followerStorage) AppendRootEvent(rootevent.Event) error   { return nil }
func (f *followerStorage) SaveAllocatorState(uint64, uint64) error { return nil }
func (f *followerStorage) Refresh() error                          { return nil }
func (f *followerStorage) Close() error                            { return nil }
func (f *followerStorage) IsLeader() bool                          { return false }
func (f *followerStorage) LeaderID() uint64                        { return 2 }

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
