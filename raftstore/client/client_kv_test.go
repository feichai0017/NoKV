package client

import (
	"context"
	"fmt"
	"testing"

	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metawire "github.com/feichai0017/NoKV/meta/wire"
)

type scriptedKVService struct {
	mockService
	commitFn         func(context.Context, *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error)
	resolveLockFn    func(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error)
	checkTxnStatusFn func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error)
}

func (s *scriptedKVService) KvCommit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	if s.commitFn != nil {
		return s.commitFn(ctx, req)
	}
	return s.mockService.KvCommit(ctx, req)
}

func (s *scriptedKVService) KvResolveLock(ctx context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
	if s.resolveLockFn != nil {
		return s.resolveLockFn(ctx, req)
	}
	return s.mockService.KvResolveLock(ctx, req)
}

func (s *scriptedKVService) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
	if s.checkTxnStatusFn != nil {
		return s.checkTxnStatusFn(ctx, req)
	}
	return s.mockService.KvCheckTxnStatus(ctx, req)
}

func TestClientCommitRegionReturnsKeyError(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers: []*metapb.RegionPeer{
				{StoreId: 1, PeerId: 101},
			},
		},
		leaderStore: 1,
	})

	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		commitFn: func(context.Context, *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
			return &kvrpcpb.KvCommitResponse{
				Response: &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{Abort: "commit failed"},
				},
			}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 2, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	cli.mu.Lock()
	cli.upsertRegionLocked(metawire.DescriptorFromProto(cluster.regions[1].meta), 1)
	cli.mu.Unlock()

	err = cli.commitRegion(context.Background(), 1, [][]byte{[]byte("alfa")}, 11, 22)
	require.ErrorContains(t, err, "commit key error")
}

func TestClientResolveLocksRetriesOnNotLeader(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers: []*metapb.RegionPeer{
				{StoreId: 1, PeerId: 101},
				{StoreId: 2, PeerId: 202},
			},
		},
		leaderStore: 1,
	})

	var firstCalls int
	addr1, stop1 := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		resolveLockFn: func(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
			firstCalls++
			return &kvrpcpb.KvResolveLockResponse{
				RegionError: &errorpb.RegionError{
					NotLeader: &errorpb.NotLeader{
						RegionId: 1,
						Leader:   leaderPeer(cluster.regions[1].meta, 2),
					},
				},
			}, nil
		},
	})
	defer stop1()

	addr2, stop2 := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 2, cluster: cluster},
		resolveLockFn: func(_ context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
			return &kvrpcpb.KvResolveLockResponse{
				Response: &kvrpcpb.ResolveLockResponse{
					ResolvedLocks: uint64(len(req.GetRequest().GetKeys())),
				},
			}, nil
		},
	})
	defer stop2()

	cli, err := New(Config{
		Stores: []StoreEndpoint{
			{StoreID: 1, Addr: addr1},
			{StoreID: 2, Addr: addr2},
		},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 3, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resolved, err := cli.ResolveLocks(context.Background(), 9, 10, [][]byte{[]byte("alfa"), []byte("bravo")})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resolved)
	require.Equal(t, 1, firstCalls)

	cli.mu.RLock()
	require.Equal(t, uint64(2), cli.regions[1].leader)
	cli.mu.RUnlock()
}

func TestClientCheckTxnStatusRetriesOnNotLeader(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers: []*metapb.RegionPeer{
				{StoreId: 1, PeerId: 101},
				{StoreId: 2, PeerId: 202},
			},
		},
		leaderStore: 1,
	})

	var firstCalls int
	addr1, stop1 := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		checkTxnStatusFn: func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
			firstCalls++
			return &kvrpcpb.KvCheckTxnStatusResponse{
				RegionError: &errorpb.RegionError{
					NotLeader: &errorpb.NotLeader{
						RegionId: 1,
						Leader:   leaderPeer(cluster.regions[1].meta, 2),
					},
				},
			}, nil
		},
	})
	defer stop1()

	addr2, stop2 := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 2, cluster: cluster},
		checkTxnStatusFn: func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
			return &kvrpcpb.KvCheckTxnStatusResponse{
				Response: &kvrpcpb.CheckTxnStatusResponse{
					CommitVersion: 77,
					Action:        kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
				},
			}, nil
		},
	})
	defer stop2()

	cli, err := New(Config{
		Stores: []StoreEndpoint{
			{StoreID: 1, Addr: addr1},
			{StoreID: 2, Addr: addr2},
		},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 3, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resp, err := cli.CheckTxnStatus(context.Background(), []byte("alfa"), 10, 20)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(77), resp.GetCommitVersion())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, resp.GetAction())
	require.Equal(t, 1, firstCalls)

	cli.mu.RLock()
	require.Equal(t, uint64(2), cli.regions[1].leader)
	cli.mu.RUnlock()
}

func TestClientResolveRegionLocksReturnsKeyError(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers: []*metapb.RegionPeer{
				{StoreId: 1, PeerId: 101},
			},
		},
		leaderStore: 1,
	})

	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		resolveLockFn: func(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
			return &kvrpcpb.KvResolveLockResponse{
				Response: &kvrpcpb.ResolveLockResponse{
					Error: &kvrpcpb.KeyError{Abort: "resolve failed"},
				},
			}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 2, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	cli.mu.Lock()
	cli.upsertRegionLocked(metawire.DescriptorFromProto(cluster.regions[1].meta), 1)
	cli.mu.Unlock()

	_, err = cli.resolveRegionLocks(context.Background(), 1, 10, 11, [][]byte{[]byte("alfa")})
	require.ErrorContains(t, err, "resolve lock key error")
}

func TestClientTwoPhaseCommitRejectsMissingPrimaryMutation(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 101},
				},
			},
			leaderStore: 1,
		},
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 2,
				StartKey: []byte("m"),
				EndKey:   []byte("z"),
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 201},
				},
			},
			leaderStore: 1,
		},
	)
	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	err = cli.TwoPhaseCommit(context.Background(), []byte("omega"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value")},
	}, 1, 2, 3)
	require.EqualError(t, err, fmt.Sprintf("client: primary key %q missing from mutations", []byte("omega")))
}
