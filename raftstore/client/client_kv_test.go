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
	prewriteFn       func(context.Context, *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error)
	commitFn         func(context.Context, *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error)
	rollbackFn       func(context.Context, *kvrpcpb.KvBatchRollbackRequest) (*kvrpcpb.KvBatchRollbackResponse, error)
	resolveLockFn    func(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error)
	checkTxnStatusFn func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error)
}

func (s *scriptedKVService) KvPrewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	if s.prewriteFn != nil {
		return s.prewriteFn(ctx, req)
	}
	return s.mockService.KvPrewrite(ctx, req)
}

func (s *scriptedKVService) KvCommit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	if s.commitFn != nil {
		return s.commitFn(ctx, req)
	}
	return s.mockService.KvCommit(ctx, req)
}

func (s *scriptedKVService) KvBatchRollback(ctx context.Context, req *kvrpcpb.KvBatchRollbackRequest) (*kvrpcpb.KvBatchRollbackResponse, error) {
	if s.rollbackFn != nil {
		return s.rollbackFn(ctx, req)
	}
	return s.mockService.KvBatchRollback(ctx, req)
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
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
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
		StoreResolver: staticStoreResolver{
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
		StoreResolver: staticStoreResolver{
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

func TestClientResolveLocksReturnsKeyError(t *testing.T) {
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
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 2, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	_, err = cli.ResolveLocks(context.Background(), 10, 11, [][]byte{[]byte("alfa")})
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
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
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

func TestClientTwoPhaseCommitRollsBackPrewritesAfterSecondaryPrewriteFailure(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
			},
			leaderStore: 1,
		},
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 201}},
			},
			leaderStore: 1,
		},
	)
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
	svc.prewriteFn = func(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
		if req.GetContext().GetRegionId() == 2 {
			return &kvrpcpb.KvPrewriteResponse{
				Response: &kvrpcpb.PrewriteResponse{
					Errors: []*kvrpcpb.KeyError{{Abort: "secondary prewrite failed"}},
				},
			}, nil
		}
		return svc.mockService.KvPrewrite(ctx, req)
	}
	addr, stop := startBlockingStore(t, svc)
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 2, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	err = cli.TwoPhaseCommit(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
	}, 30, 31, 3000)
	require.ErrorContains(t, err, "secondary prewrite failed")

	cluster.mu.Lock()
	_, primaryPending := cluster.regions[1].pending[30]
	primaryRollbackHits := cluster.regions[1].rollbackHits
	cluster.mu.Unlock()
	require.False(t, primaryPending, "primary prewrite must be rolled back")
	require.Equal(t, 1, primaryRollbackHits)
}

func TestClientTwoPhaseCommitResolvesSecondariesAfterSecondaryCommitFailure(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
			},
			leaderStore: 1,
		},
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 201}},
			},
			leaderStore: 1,
		},
	)
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
	svc.commitFn = func(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
		if req.GetContext().GetRegionId() == 2 {
			return &kvrpcpb.KvCommitResponse{
				Response: &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{Abort: "secondary commit failed"},
				},
			}, nil
		}
		return svc.mockService.KvCommit(ctx, req)
	}
	addr, stop := startBlockingStore(t, svc)
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 2, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	err = cli.TwoPhaseCommit(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
	}, 40, 41, 3000)
	require.NoError(t, err)

	cluster.mu.Lock()
	secondary := cluster.regions[2]
	_, pending := secondary.pending[40]
	committed := secondary.committed["omega"]
	resolveHits := secondary.resolveHits
	cluster.mu.Unlock()
	require.False(t, pending, "secondary locks must be resolved after primary commit")
	require.Equal(t, []byte("v2"), committed.value)
	require.Equal(t, uint64(41), committed.commitVersion)
	require.Equal(t, 1, resolveHits)
}

func TestClientTwoPhaseCommitReportsSecondaryCommitAndResolveFailures(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
			},
			leaderStore: 1,
		},
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 201}},
			},
			leaderStore: 1,
		},
	)
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
	svc.commitFn = func(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
		if req.GetContext().GetRegionId() == 2 {
			return &kvrpcpb.KvCommitResponse{
				Response: &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{Abort: "secondary commit failed"},
				},
			}, nil
		}
		return svc.mockService.KvCommit(ctx, req)
	}
	svc.resolveLockFn = func(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
		return &kvrpcpb.KvResolveLockResponse{
			Response: &kvrpcpb.ResolveLockResponse{
				Error: &kvrpcpb.KeyError{Abort: "resolve failed"},
			},
		}, nil
	}
	addr, stop := startBlockingStore(t, svc)
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 2, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	err = cli.TwoPhaseCommit(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
	}, 45, 46, 3000)
	require.Error(t, err)
	require.ErrorContains(t, err, "secondary commit failed")
	require.ErrorContains(t, err, "resolve committed secondaries")
	require.ErrorContains(t, err, "resolve failed")
}

func TestClientTwoPhaseCommitReroutesAfterPrewriteEpochMismatch(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   nil,
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
		},
		leaderStore: 1,
	})
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
	var splitOnce bool
	svc.prewriteFn = func(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
		if !splitOnce && req.GetContext().GetRegionId() == 1 {
			splitOnce = true
			regionErr := installSplitRegionsForTest(cluster)
			return &kvrpcpb.KvPrewriteResponse{RegionError: regionErr}, nil
		}
		return svc.mockService.KvPrewrite(ctx, req)
	}
	addr, stop := startBlockingStore(t, svc)
	defer stop()

	cli, err := New(Config{
		StoreResolver: staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: &mockRegionResolver{region: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   nil,
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 4, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	err = cli.TwoPhaseCommit(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
	}, 50, 51, 3000)
	require.NoError(t, err)

	cluster.mu.Lock()
	region11 := cluster.regions[11]
	region12 := cluster.regions[12]
	cluster.mu.Unlock()
	require.Equal(t, []byte("v1"), region11.committed["alfa"].value)
	require.Equal(t, []byte("v2"), region12.committed["omega"].value)
}

func TestClientTwoPhaseCommitReroutesSecondaryCommitAfterEpochMismatch(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
			},
			leaderStore: 1,
		},
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 201}},
			},
			leaderStore: 1,
		},
	)
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
	var epochOnce bool
	svc.commitFn = func(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
		if !epochOnce && req.GetContext().GetRegionId() == 2 {
			epochOnce = true
			regionErr := replaceSecondaryRegionForTest(cluster)
			return &kvrpcpb.KvCommitResponse{RegionError: regionErr}, nil
		}
		return svc.mockService.KvCommit(ctx, req)
	}
	addr, stop := startBlockingStore(t, svc)
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 4, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	err = cli.TwoPhaseCommit(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
	}, 60, 61, 3000)
	require.NoError(t, err)

	cluster.mu.Lock()
	secondary := cluster.regions[22]
	_, pending := secondary.pending[60]
	committed := secondary.committed["omega"]
	cluster.mu.Unlock()
	require.False(t, pending)
	require.Equal(t, []byte("v2"), committed.value)
	require.Equal(t, uint64(61), committed.commitVersion)
}

func installSplitRegionsForTest(cluster *mockCluster) *errorpb.RegionError {
	left := &clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 11,
			StartKey: []byte("a"),
			EndKey:   []byte("m"),
			Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 111}},
		},
		leaderStore: 1,
		pending:     make(map[uint64]map[string]clusterPending),
		committed:   make(map[string]clusterValue),
	}
	right := &clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 12,
			StartKey: []byte("m"),
			EndKey:   nil,
			Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 121}},
		},
		leaderStore: 1,
		pending:     make(map[uint64]map[string]clusterPending),
		committed:   make(map[string]clusterValue),
	}
	cluster.mu.Lock()
	delete(cluster.regions, 1)
	cluster.regions[11] = left
	cluster.regions[12] = right
	cluster.mu.Unlock()
	return &errorpb.RegionError{EpochNotMatch: &errorpb.EpochNotMatch{Regions: []*metapb.RegionDescriptor{
		protoClone(left.meta),
		protoClone(right.meta),
	}}}
}

func replaceSecondaryRegionForTest(cluster *mockCluster) *errorpb.RegionError {
	cluster.mu.Lock()
	old := cluster.regions[2]
	delete(cluster.regions, 2)
	replacement := &clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 22,
			StartKey: []byte("m"),
			EndKey:   nil,
			Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 221}},
		},
		leaderStore: 1,
		pending:     old.pending,
		committed:   old.committed,
	}
	cluster.regions[22] = replacement
	cluster.mu.Unlock()
	return &errorpb.RegionError{EpochNotMatch: &errorpb.EpochNotMatch{Regions: []*metapb.RegionDescriptor{
		protoClone(replacement.meta),
	}}}
}
