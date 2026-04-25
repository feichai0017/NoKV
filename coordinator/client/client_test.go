package client

import (
	"context"
	"errors"
	coordablation "github.com/feichai0017/NoKV/coordinator/ablation"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
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

	joinResp, err := cli.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.StoreJoined(1)),
	})
	require.NoError(t, err)
	require.True(t, joinResp.GetAccepted())

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
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(
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
		Event: metawire.RootEventToProto(rootevent.PeerAdded(
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
	require.Equal(t, "peer:11:add:2:201", publishResp.GetAssessment().GetTransitionId())
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

	followerSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), &followerStorage{})
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

func (f *followerStorage) Load() (rootview.Snapshot, error) {
	return rootview.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
}
func (f *followerStorage) AppendRootEvent(context.Context, rootevent.Event) error { return nil }
func (f *followerStorage) SaveAllocatorState(context.Context, uint64, uint64) error {
	return nil
}
func (f *followerStorage) ApplyTenure(context.Context, rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	return rootstate.SuccessionState{}, nil
}
func (f *followerStorage) ApplyHandover(context.Context, rootproto.HandoverCommand) (rootstate.SuccessionState, error) {
	return rootstate.SuccessionState{}, nil
}
func (f *followerStorage) Refresh() error   { return nil }
func (f *followerStorage) Close() error     { return nil }
func (f *followerStorage) IsLeader() bool   { return false }
func (f *followerStorage) LeaderID() uint64 { return 2 }

func TestGRPCClientDoesNotRetryReadOnNotLeaderWriteError(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, errNotLeaderPrefix+" (leader_id=2)")
	require.True(t, retryableWrite(err))
	require.False(t, retryableRead(err))
	require.True(t, IsNotLeader(err))
	leaderID, ok := LeaderHint(err)
	require.True(t, ok)
	require.Equal(t, uint64(2), leaderID)
}

func TestCoordinatorClientErrorHelpers(t *testing.T) {
	require.True(t, IsEmptyAddress(errEmptyAddress))
	require.True(t, IsNoReachableAddress(errNoReachableAddress))
	require.True(t, IsConnectionShutdown(errConnectionShutdown))
	require.True(t, IsStaleWitnessEra(errStaleWitnessEra))
	require.True(t, IsInvalidWitness(errInvalidWitness))
	require.False(t, IsNotLeader(errEmptyAddress))
	require.False(t, IsLeaseNotHeld(errEmptyAddress))
	_, ok := LeaderHint(errEmptyAddress)
	require.False(t, ok)
}

func TestGRPCClientRetriesWriteOnLeaseNotHeld(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, errLeaseNotHeldPrefix+": meta/root/state: coordinator lease held")
	require.True(t, IsLeaseNotHeld(err))
	require.True(t, retryableWrite(err))
	require.False(t, retryableRead(err))
	require.False(t, IsNotLeader(err))
}

func TestGRPCClientRejectsInvalidAllocWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"alloc-invalid"}, map[string]*scriptedCoordinatorServer{
		"alloc-invalid": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          10,
					Count:            2,
					Era:              1,
					ConsumedFrontier: 10,
				},
			},
		},
	})

	_, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "consumed_frontier=10 expected=11")
}

func TestGRPCClientRetriesStaleWitnessEraAcrossEndpoints(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"fresh": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          100,
					Count:            1,
					Era:              2,
					ConsumedFrontier: 100,
				},
				{
					FirstId:          101,
					Count:            1,
					Era:              2,
					ConsumedFrontier: 101,
				},
			},
		},
		"stale": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          50,
					Count:            1,
					Era:              1,
					ConsumedFrontier: 50,
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"fresh", "stale"}, servers)

	resp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), resp.GetFirstId())

	cli.markPreferred("passthrough:///stale")

	resp, err = cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(101), resp.GetFirstId())
	require.Equal(t, 1, servers["stale"].allocCalls)
	require.Equal(t, 2, servers["fresh"].allocCalls)
}

func TestGRPCClientRejectsInvalidTSOWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"tso-invalid"}, map[string]*scriptedCoordinatorServer{
		"tso-invalid": {
			tsoResponses: []*coordpb.TsoResponse{
				{
					Timestamp:        90,
					Count:            1,
					Era:              3,
					ConsumedFrontier: 89,
				},
			},
		},
	})

	_, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "consumed_frontier=89 expected=90")
}

func TestGRPCClientRejectsInvalidMetadataWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"metadata-invalid"}, map[string]*scriptedCoordinatorServer{
		"metadata-invalid": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 7},
					ServedRootToken:            &coordpb.RootToken{Term: 1, Index: 4, Revision: 4},
					CurrentRootToken:           &coordpb.RootToken{Term: 1, Index: 5, Revision: 5},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         7,
					RequiredDescriptorRevision: 7,
					Era:                        2,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	})

	_, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 1, Index: 5, Revision: 5},
		RequiredDescriptorRevision: 7,
		MaxRootLag:                 new(uint64(2)),
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "served_root_token does not satisfy required_root_token")
}

func TestGRPCClientAcceptsValidMetadataWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"metadata-valid"}, map[string]*scriptedCoordinatorServer{
		"metadata-valid": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	})

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 new(uint64(2)),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
}

func TestGRPCClientRetriesStaleMetadataWitnessEraAcrossEndpoints(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"fresh": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 11},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
		"stale": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 10, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        2,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"fresh", "stale"}, servers)

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 new(uint64(2)),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())

	cli.markPreferred("passthrough:///stale")

	resp, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 new(uint64(2)),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(12), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, 1, servers["stale"].getCalls)
	require.Equal(t, 2, servers["fresh"].getCalls)
}

func TestGRPCClientAcceptsZeroEraMetadataWitnessAfterDetachedEra(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
			},
		},
	})

	_, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 new(uint64(2)),
	})
	require.NoError(t, err)

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(12), resp.GetRegionDescriptor().GetRegionId())
	require.Zero(t, resp.GetEra())
}

func TestGRPCClientRejectsZeroEraMetadataWitnessRegressingAttachedFrontier(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
			},
		},
	})

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(12), resp.GetRegionDescriptor().GetRegionId())

	_, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "current_root_token regressed behind attached floor")
}

func TestGRPCClientRejectsZeroEraMetadataWitnessWithoutAuthoritativeAttachedServing(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 11, Revision: 11},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BEST_EFFORT,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	})

	_, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 new(uint64(2)),
	})
	require.NoError(t, err)

	_, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BEST_EFFORT,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "era=0 requires authoritative attached")
}

func TestGRPCClientRejectsSuppressedReplyEvidence(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          100,
					Count:            1,
					Era:              rootproto.MandateWitnessEraSuppressed,
					ConsumedFrontier: 0,
				},
			},
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        rootproto.MandateWitnessEraSuppressed,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
			},
		},
	})

	_, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "reply evidence suppressed")

	_, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "reply evidence suppressed")
}

func TestGRPCClientRejectsReplyAtObservedSealFloor(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:           100,
					Count:             1,
					Era:               2,
					ConsumedFrontier:  100,
					ObservedLegacyEra: 2,
				},
			},
		},
	})

	_, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.True(t, IsStaleWitnessEra(err))
	require.Contains(t, err.Error(), "sealed_floor=2")
}

func TestGRPCClientAblationDisableClientVerifyAcceptsStaleEra(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"fresh": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          100,
					Count:            1,
					Era:              2,
					ConsumedFrontier: 100,
				},
			},
		},
		"stale": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          50,
					Count:            1,
					Era:              1,
					ConsumedFrontier: 50,
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"fresh", "stale"}, servers)
	require.NoError(t, cli.ConfigureAblation(coordablation.Config{DisableClientVerify: true}))

	resp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), resp.GetFirstId())

	cli.markPreferred("passthrough:///stale")

	resp, err = cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(50), resp.GetFirstId())
	require.Equal(t, uint64(1), resp.GetEra())
}

type scriptedCoordinatorServer struct {
	coordpb.UnimplementedCoordinatorServer

	mu sync.Mutex

	storeResponses []*coordpb.StoreHeartbeatResponse
	storeErrors    []error
	storeCalls     int

	listResponses []*coordpb.ListTransitionsResponse
	listErrors    []error
	listCalls     int

	assessResponses []*coordpb.AssessRootEventResponse
	assessErrors    []error
	assessCalls     int

	allocResponses []*coordpb.AllocIDResponse
	allocErrors    []error
	allocCalls     int

	tsoResponses []*coordpb.TsoResponse
	tsoErrors    []error
	tsoCalls     int

	getResponses []*coordpb.GetRegionByKeyResponse
	getErrors    []error
	getCalls     int
}

func (s *scriptedCoordinatorServer) StoreHeartbeat(_ context.Context, _ *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storeCalls++
	var err error
	if len(s.storeErrors) > 0 {
		err = s.storeErrors[0]
		s.storeErrors = s.storeErrors[1:]
	}
	if len(s.storeResponses) == 0 {
		return nil, err
	}
	resp := s.storeResponses[0]
	s.storeResponses = s.storeResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) ListTransitions(_ context.Context, _ *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	var err error
	if len(s.listErrors) > 0 {
		err = s.listErrors[0]
		s.listErrors = s.listErrors[1:]
	}
	if len(s.listResponses) == 0 {
		return nil, err
	}
	resp := s.listResponses[0]
	s.listResponses = s.listResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) AssessRootEvent(_ context.Context, _ *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assessCalls++
	var err error
	if len(s.assessErrors) > 0 {
		err = s.assessErrors[0]
		s.assessErrors = s.assessErrors[1:]
	}
	if len(s.assessResponses) == 0 {
		return nil, err
	}
	resp := s.assessResponses[0]
	s.assessResponses = s.assessResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) AllocID(_ context.Context, _ *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocCalls++
	var err error
	if len(s.allocErrors) > 0 {
		err = s.allocErrors[0]
		s.allocErrors = s.allocErrors[1:]
	}
	if len(s.allocResponses) == 0 {
		return nil, err
	}
	resp := s.allocResponses[0]
	s.allocResponses = s.allocResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) Tso(_ context.Context, _ *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tsoCalls++
	var err error
	if len(s.tsoErrors) > 0 {
		err = s.tsoErrors[0]
		s.tsoErrors = s.tsoErrors[1:]
	}
	if len(s.tsoResponses) == 0 {
		return nil, err
	}
	resp := s.tsoResponses[0]
	s.tsoResponses = s.tsoResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) GetRegionByKey(_ context.Context, _ *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	var err error
	if len(s.getErrors) > 0 {
		err = s.getErrors[0]
		s.getErrors = s.getErrors[1:]
	}
	if len(s.getResponses) == 0 {
		return nil, err
	}
	resp := s.getResponses[0]
	s.getResponses = s.getResponses[1:]
	return resp, err
}

func newScriptedCoordinatorClient(t *testing.T, order []string, servers map[string]*scriptedCoordinatorServer) *GRPCClient {
	t.Helper()

	const bufSize = 1 << 20
	listeners := make(map[string]*bufconn.Listener, len(order))
	for _, name := range order {
		srv := servers[name]
		require.NotNil(t, srv, "missing scripted server %q", name)
		listener := bufconn.Listen(bufSize)
		listeners[name] = listener
		t.Cleanup(func() {
			_ = listener.Close()
		})

		grpcServer := grpc.NewServer()
		coordpb.RegisterCoordinatorServer(grpcServer, srv)
		go func(l *bufconn.Listener) {
			_ = grpcServer.Serve(l)
		}(listener)
		t.Cleanup(grpcServer.GracefulStop)
	}

	dialer := func(_ context.Context, target string) (net.Conn, error) {
		name := strings.TrimPrefix(target, "passthrough:///")
		listener, ok := listeners[name]
		if !ok {
			return nil, errors.New("unknown target: " + target)
		}
		return listener.Dial()
	}

	addrs := make([]string, 0, len(order))
	for _, name := range order {
		addrs = append(addrs, "passthrough:///"+name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewGRPCClient(ctx, strings.Join(addrs, ","),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func TestGRPCClientStoreHeartbeatBroadcastsAndPrefersOperationalResponse(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"standby": {
			storeResponses: []*coordpb.StoreHeartbeatResponse{
				{Accepted: true},
			},
		},
		"holder": {
			storeResponses: []*coordpb.StoreHeartbeatResponse{
				{
					Accepted: true,
					Operations: []*coordpb.SchedulerOperation{
						{
							Type:         coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
							RegionId:     9,
							SourcePeerId: 101,
							TargetPeerId: 201,
						},
					},
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"standby", "holder"}, servers)

	resp, err := cli.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:         2,
		RegionNum:       2,
		LeaderNum:       1,
		LeaderRegionIds: []uint64{9},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetOperations(), 1)
	require.Equal(t, uint64(9), resp.GetOperations()[0].GetRegionId())
	require.Equal(t, 1, servers["standby"].storeCalls)
	require.Equal(t, 1, servers["holder"].storeCalls)
	require.Equal(t, "passthrough:///holder", cli.orderedEndpoints()[0].addr)
}

func TestGRPCClientListTransitionsAndAssessRootEvent(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"holder": {
			listResponses: []*coordpb.ListTransitionsResponse{{
				Entries: []*coordpb.TransitionEntry{{
					Key:          7,
					TransitionId: "peer:7:add:2:201",
				}},
			}},
			assessResponses: []*coordpb.AssessRootEventResponse{{
				Assessment: &coordpb.TransitionAssessment{
					Key:          7,
					Decision:     coordpb.TransitionDecision_TRANSITION_DECISION_APPLY,
					TransitionId: "peer:7:add:2:201",
				},
			}},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"holder"}, servers)

	listResp, err := cli.ListTransitions(context.Background(), &coordpb.ListTransitionsRequest{})
	require.NoError(t, err)
	require.Len(t, listResp.GetEntries(), 1)
	require.Equal(t, "peer:7:add:2:201", listResp.GetEntries()[0].GetTransitionId())

	assessResp, err := cli.AssessRootEvent(context.Background(), &coordpb.AssessRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionTombstoned(7)),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, assessResp.GetAssessment().GetDecision())
	require.Equal(t, 1, servers["holder"].listCalls)
	require.Equal(t, 1, servers["holder"].assessCalls)
}

func TestClientHelperFunctions(t *testing.T) {
	addrs, err := splitAddresses("  a , b ,, c ")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, addrs)
	_, err = splitAddresses(" , ")
	require.ErrorIs(t, err, errEmptyAddress)

	defaultOpts := normalizeDialOptions(nil)
	require.NotEmpty(t, defaultOpts)

	custom := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	require.Equal(t, custom, normalizeDialOptions(custom))

	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	t.Cleanup(func() {
		_ = listener.Close()
	})
	grpcServer := grpc.NewServer()
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.GracefulStop)

	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	require.NoError(t, waitForReady(ctx, conn))

	closeAllEndpoints([]grpcEndpoint{{addr: "passthrough:///bufnet", conn: conn}})
	require.Eventually(t, func() bool {
		return conn.GetState() == connectivity.Shutdown
	}, time.Second, 10*time.Millisecond)
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
