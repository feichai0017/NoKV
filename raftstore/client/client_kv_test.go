package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
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
	getFn             func(context.Context, *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error)
	batchGetFn        func(context.Context, *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error)
	scanFn            func(context.Context, *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error)
	prewriteFn        func(context.Context, *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error)
	commitFn          func(context.Context, *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error)
	rollbackFn        func(context.Context, *kvrpcpb.KvBatchRollbackRequest) (*kvrpcpb.KvBatchRollbackResponse, error)
	resolveLockFn     func(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error)
	checkTxnStatusFn  func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error)
	txnHeartBeatFn    func(context.Context, *kvrpcpb.KvTxnHeartBeatRequest) (*kvrpcpb.KvTxnHeartBeatResponse, error)
	tryAtomicMutateFn func(context.Context, *kvrpcpb.KvTryAtomicMutateRequest) (*kvrpcpb.KvTryAtomicMutateResponse, error)
}

func (s *scriptedKVService) Get(ctx context.Context, req *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	if s.getFn != nil {
		return s.getFn(ctx, req)
	}
	return s.mockService.Get(ctx, req)
}

func (s *scriptedKVService) BatchGet(ctx context.Context, req *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error) {
	if s.batchGetFn != nil {
		return s.batchGetFn(ctx, req)
	}
	return s.mockService.BatchGet(ctx, req)
}

func (s *scriptedKVService) Scan(ctx context.Context, req *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
	if s.scanFn != nil {
		return s.scanFn(ctx, req)
	}
	return s.mockService.Scan(ctx, req)
}

func (s *scriptedKVService) Prewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	if s.prewriteFn != nil {
		return s.prewriteFn(ctx, req)
	}
	return s.mockService.Prewrite(ctx, req)
}

func (s *scriptedKVService) Commit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	if s.commitFn != nil {
		return s.commitFn(ctx, req)
	}
	return s.mockService.Commit(ctx, req)
}

func (s *scriptedKVService) BatchRollback(ctx context.Context, req *kvrpcpb.KvBatchRollbackRequest) (*kvrpcpb.KvBatchRollbackResponse, error) {
	if s.rollbackFn != nil {
		return s.rollbackFn(ctx, req)
	}
	return s.mockService.BatchRollback(ctx, req)
}

func (s *scriptedKVService) ResolveLock(ctx context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
	if s.resolveLockFn != nil {
		return s.resolveLockFn(ctx, req)
	}
	return s.mockService.ResolveLock(ctx, req)
}

func (s *scriptedKVService) CheckTxnStatus(ctx context.Context, req *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
	if s.checkTxnStatusFn != nil {
		return s.checkTxnStatusFn(ctx, req)
	}
	return s.mockService.CheckTxnStatus(ctx, req)
}

func (s *scriptedKVService) TxnHeartBeat(ctx context.Context, req *kvrpcpb.KvTxnHeartBeatRequest) (*kvrpcpb.KvTxnHeartBeatResponse, error) {
	if s.txnHeartBeatFn != nil {
		return s.txnHeartBeatFn(ctx, req)
	}
	return s.mockService.TxnHeartBeat(ctx, req)
}

func (s *scriptedKVService) TryAtomicMutate(ctx context.Context, req *kvrpcpb.KvTryAtomicMutateRequest) (*kvrpcpb.KvTryAtomicMutateResponse, error) {
	if s.tryAtomicMutateFn != nil {
		return s.tryAtomicMutateFn(ctx, req)
	}
	return s.mockService.TryAtomicMutate(ctx, req)
}

func TestClientTryAtomicMutateStatsRecordRouteFallback(t *testing.T) {
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
	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: "unused"}},
		RegionResolver: resolverFromCluster(cluster),
		Retry:          RetryPolicy{MaxAttempts: 2, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	handled, err := cli.TryAtomicMutate(context.Background(), []byte("alfa"), nil, []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
	}, 10, 11)
	require.NoError(t, err)
	require.False(t, handled)
	stats := cli.Stats()
	require.Equal(t, uint64(0), stats["atomic_route_single_total"])
	require.Equal(t, uint64(1), stats["atomic_route_multi_total"])
	require.Equal(t, uint64(0), stats["atomic_local_fallback_total"])
	require.Equal(t, uint64(0), stats["atomic_success_total"])
}

func TestClientTryAtomicMutateStatsRecordLocalFallbackAndSuccess(t *testing.T) {
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
	var fallback bool
	svc.tryAtomicMutateFn = func(context.Context, *kvrpcpb.KvTryAtomicMutateRequest) (*kvrpcpb.KvTryAtomicMutateResponse, error) {
		if fallback {
			return &kvrpcpb.KvTryAtomicMutateResponse{
				Response: &kvrpcpb.TryAtomicMutateResponse{FallbackToTwoPhaseCommit: true},
			}, nil
		}
		return &kvrpcpb.KvTryAtomicMutateResponse{
			Response: &kvrpcpb.TryAtomicMutateResponse{AppliedKeys: 1},
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

	fallback = true
	handled, err := cli.TryAtomicMutate(context.Background(), []byte("alfa"), nil, []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
	}, 10, 11)
	require.NoError(t, err)
	require.False(t, handled)

	fallback = false
	handled, err = cli.TryAtomicMutate(context.Background(), []byte("alfa"), nil, []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
	}, 12, 13)
	require.NoError(t, err)
	require.True(t, handled)

	stats := cli.Stats()
	require.Equal(t, uint64(2), stats["atomic_route_single_total"])
	require.Equal(t, uint64(0), stats["atomic_route_multi_total"])
	require.Equal(t, uint64(1), stats["atomic_local_fallback_total"])
	require.Equal(t, uint64(1), stats["atomic_success_total"])
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
					Error: &kvrpcpb.KeyError{CommitTsExpired: &kvrpcpb.CommitTsExpired{
						Key:         []byte("alfa"),
						CommitTs:    22,
						MinCommitTs: 30,
					}},
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
	txnErr, ok := nokverrors.AsTxnKeyError(err)
	require.True(t, ok)
	require.Len(t, txnErr.Errors, 1)
	require.NotNil(t, txnErr.Errors[0].GetCommitTsExpired())
	require.Equal(t, uint64(30), txnErr.Errors[0].GetCommitTsExpired().GetMinCommitTs())
}

func TestClientTwoPhaseCommitPrewriteRetryBudgetExhaustion(t *testing.T) {
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

	var prewriteCalls int
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		prewriteFn: func(context.Context, *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
			prewriteCalls++
			return &kvrpcpb.KvPrewriteResponse{
				RegionError: &errorpb.RegionError{
					NotLeader: &errorpb.NotLeader{RegionId: 1},
				},
			}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:        1,
			RegionErrorBackoff: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = cli.TwoPhaseCommit(ctx, []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
	}, 10, 11, 3000)

	require.Error(t, err)
	require.True(t, IsRetryExhausted(err), "expected retry budget exhaustion, got %v", err)
	require.False(t, errors.Is(err, context.DeadlineExceeded))
	require.Equal(t, 1, prewriteCalls)
}

func TestClientTwoPhaseCommitPrewriteDeadlineDuringRetryBackoff(t *testing.T) {
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

	var prewriteCalls int
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		prewriteFn: func(context.Context, *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
			prewriteCalls++
			return &kvrpcpb.KvPrewriteResponse{
				RegionError: &errorpb.RegionError{
					NotLeader: &errorpb.NotLeader{RegionId: 1},
				},
			}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:        2,
			RegionErrorBackoff: time.Second,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = cli.TwoPhaseCommit(ctx, []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
	}, 10, 11, 3000)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, IsRetryExhausted(err), "deadline should not be flattened into retry exhaustion")
	require.Equal(t, 1, prewriteCalls)
}

func TestClientGetResolvesCommittedLockAndRetries(t *testing.T) {
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

	var getCalls int
	var checkCalls int
	var resolveCalls int
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		getFn: func(context.Context, *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
			getCalls++
			if getCalls == 1 {
				return &kvrpcpb.KvGetResponse{
					Response: &kvrpcpb.GetResponse{
						Error: &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
							PrimaryLock: []byte("primary"),
							Key:         []byte("alfa"),
							LockVersion: 10,
						}},
					},
				}, nil
			}
			return &kvrpcpb.KvGetResponse{
				Response: &kvrpcpb.GetResponse{Value: []byte("visible")},
			}, nil
		},
		checkTxnStatusFn: func(_ context.Context, req *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
			checkCalls++
			require.Equal(t, []byte("primary"), req.GetRequest().GetPrimaryKey())
			require.Equal(t, uint64(10), req.GetRequest().GetLockTs())
			require.Equal(t, uint64(20), req.GetRequest().GetCurrentTs())
			require.Zero(t, req.GetRequest().GetCurrentTime())
			return &kvrpcpb.KvCheckTxnStatusResponse{
				Response: &kvrpcpb.CheckTxnStatusResponse{CommitVersion: 30},
			}, nil
		},
		resolveLockFn: func(_ context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
			resolveCalls++
			require.Equal(t, uint64(10), req.GetRequest().GetStartVersion())
			require.Equal(t, uint64(30), req.GetRequest().GetCommitVersion())
			require.Equal(t, [][]byte{[]byte("alfa")}, req.GetRequest().GetKeys())
			return &kvrpcpb.KvResolveLockResponse{
				Response: &kvrpcpb.ResolveLockResponse{ResolvedLocks: 1},
			}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 3, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resp, err := cli.Get(context.Background(), []byte("alfa"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("visible"), resp.GetValue())
	require.Equal(t, 2, getCalls)
	require.Equal(t, 1, checkCalls)
	require.Equal(t, 1, resolveCalls)
}

func TestClientGetReturnsLiveLockAfterRetryBudget(t *testing.T) {
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

	var checkCalls int
	var resolveCalls int
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		getFn: func(context.Context, *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
			return &kvrpcpb.KvGetResponse{
				Response: &kvrpcpb.GetResponse{
					Error: &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
						PrimaryLock: []byte("primary"),
						Key:         []byte("alfa"),
						LockVersion: 10,
					}},
				},
			}, nil
		},
		checkTxnStatusFn: func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
			checkCalls++
			return &kvrpcpb.KvCheckTxnStatusResponse{
				Response: &kvrpcpb.CheckTxnStatusResponse{
					LockTtl: 100,
					Action:  kvrpcpb.CheckTxnStatusAction_CheckTxnStatusMinCommitTsPushed,
				},
			}, nil
		},
		resolveLockFn: func(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
			resolveCalls++
			return &kvrpcpb.KvResolveLockResponse{}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 1, LockResolveBackoff: -1},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resp, err := cli.Get(context.Background(), []byte("alfa"), 20)
	require.Nil(t, resp)
	txnErr, ok := nokverrors.AsTxnKeyError(err)
	require.True(t, ok)
	require.Len(t, txnErr.Errors, 1)
	require.NotNil(t, txnErr.Errors[0].GetLocked())
	require.Equal(t, 1, checkCalls)
	require.Equal(t, 0, resolveCalls)
}

func TestClientBatchGetResolvesCommittedLockAndKeepsCompletedReads(t *testing.T) {
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

	var batchCalls int
	var checkCalls int
	var resolveCalls int
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		batchGetFn: func(_ context.Context, req *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error) {
			batchCalls++
			responses := make([]*kvrpcpb.GetResponse, 0, len(req.GetRequest().GetRequests()))
			for _, getReq := range req.GetRequest().GetRequests() {
				switch string(getReq.GetKey()) {
				case "alfa":
					responses = append(responses, &kvrpcpb.GetResponse{Value: []byte("value-a")})
				case "bravo":
					if batchCalls == 1 {
						responses = append(responses, &kvrpcpb.GetResponse{
							Error: &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
								PrimaryLock: []byte("primary"),
								Key:         []byte("bravo"),
								LockVersion: 10,
							}},
						})
						continue
					}
					responses = append(responses, &kvrpcpb.GetResponse{Value: []byte("value-b")})
				default:
					responses = append(responses, &kvrpcpb.GetResponse{NotFound: true})
				}
			}
			return &kvrpcpb.KvBatchGetResponse{
				Response: &kvrpcpb.BatchGetResponse{Responses: responses},
			}, nil
		},
		checkTxnStatusFn: func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
			checkCalls++
			return &kvrpcpb.KvCheckTxnStatusResponse{
				Response: &kvrpcpb.CheckTxnStatusResponse{CommitVersion: 30},
			}, nil
		},
		resolveLockFn: func(_ context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
			resolveCalls++
			require.Equal(t, uint64(10), req.GetRequest().GetStartVersion())
			require.Equal(t, uint64(30), req.GetRequest().GetCommitVersion())
			require.Equal(t, [][]byte{[]byte("bravo")}, req.GetRequest().GetKeys())
			return &kvrpcpb.KvResolveLockResponse{
				Response: &kvrpcpb.ResolveLockResponse{ResolvedLocks: 1},
			}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 3, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	got, err := cli.BatchGet(context.Background(), [][]byte{[]byte("alfa"), []byte("bravo")}, 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), got["alfa"].GetValue())
	require.Equal(t, []byte("value-b"), got["bravo"].GetValue())
	require.Equal(t, 2, batchCalls)
	require.Equal(t, 1, checkCalls)
	require.Equal(t, 1, resolveCalls)
}

func TestClientScanReturnsNonLockKeyError(t *testing.T) {
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
		scanFn: func(context.Context, *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
			return &kvrpcpb.KvScanResponse{
				Response: &kvrpcpb.ScanResponse{
					Kvs: []*kvrpcpb.KV{{
						Key:   []byte("alfa"),
						Value: []byte("partial"),
					}},
					Error: &kvrpcpb.KeyError{Abort: "scan failed"},
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

	kvs, err := cli.Scan(context.Background(), []byte("alfa"), 1, 20)
	require.Nil(t, kvs)
	txnErr, ok := nokverrors.AsTxnKeyError(err)
	require.True(t, ok)
	require.Len(t, txnErr.Errors, 1)
	require.Equal(t, "scan failed", txnErr.Errors[0].GetAbort())
}

func TestClientScanResolvesCommittedLockAndRetriesWithoutPartialResult(t *testing.T) {
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

	var scanCalls int
	var checkCalls int
	var resolveCalls int
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		scanFn: func(context.Context, *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
			scanCalls++
			if scanCalls == 1 {
				return &kvrpcpb.KvScanResponse{
					Response: &kvrpcpb.ScanResponse{
						Kvs: []*kvrpcpb.KV{{
							Key:   []byte("alfa"),
							Value: []byte("partial"),
						}},
						Error: &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
							PrimaryLock: []byte("primary"),
							Key:         []byte("bravo"),
							LockVersion: 10,
						}},
					},
				}, nil
			}
			return &kvrpcpb.KvScanResponse{
				Response: &kvrpcpb.ScanResponse{
					Kvs: []*kvrpcpb.KV{{
						Key:   []byte("bravo"),
						Value: []byte("visible"),
					}},
				},
			}, nil
		},
		checkTxnStatusFn: func(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
			checkCalls++
			return &kvrpcpb.KvCheckTxnStatusResponse{
				Response: &kvrpcpb.CheckTxnStatusResponse{CommitVersion: 30},
			}, nil
		},
		resolveLockFn: func(_ context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
			resolveCalls++
			require.Equal(t, uint64(10), req.GetRequest().GetStartVersion())
			require.Equal(t, uint64(30), req.GetRequest().GetCommitVersion())
			require.Equal(t, [][]byte{[]byte("bravo")}, req.GetRequest().GetKeys())
			return &kvrpcpb.KvResolveLockResponse{
				Response: &kvrpcpb.ResolveLockResponse{ResolvedLocks: 1},
			}, nil
		},
	})
	defer stop()

	cli, err := New(Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 3, RegionErrorBackoff: 0},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	kvs, err := cli.Scan(context.Background(), []byte("alfa"), 1, 20)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("bravo"), kvs[0].GetKey())
	require.Equal(t, []byte("visible"), kvs[0].GetValue())
	require.Equal(t, 2, scanCalls)
	require.Equal(t, 1, checkCalls)
	require.Equal(t, 1, resolveCalls)
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

	resp, err := cli.CheckTxnStatus(context.Background(), []byte("alfa"), 10, 20, 30)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(77), resp.GetCommitVersion())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, resp.GetAction())
	require.Equal(t, 1, firstCalls)

	cli.mu.RLock()
	require.Equal(t, uint64(2), cli.regions[1].leader)
	cli.mu.RUnlock()
}

func TestClientTxnHeartBeatRoutesPrimaryAndDelegatesPhysicalTime(t *testing.T) {
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

	var calls int
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		txnHeartBeatFn: func(_ context.Context, req *kvrpcpb.KvTxnHeartBeatRequest) (*kvrpcpb.KvTxnHeartBeatResponse, error) {
			calls++
			require.Equal(t, uint64(1), req.GetContext().GetRegionId())
			require.Equal(t, []byte("primary"), req.GetRequest().GetPrimaryKey())
			require.Equal(t, uint64(10), req.GetRequest().GetStartVersion())
			require.Equal(t, uint64(3000), req.GetRequest().GetTtlExtension())
			require.Zero(t, req.GetRequest().GetCurrentTime())
			return &kvrpcpb.KvTxnHeartBeatResponse{
				Response: &kvrpcpb.TxnHeartBeatResponse{
					LockTtl:        5000,
					LockExpireTime: 9000,
					Action:         kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExtended,
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

	resp, err := cli.TxnHeartBeat(context.Background(), []byte("primary"), 10, 3000)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExtended, resp.GetAction())
	require.Equal(t, uint64(5000), resp.GetLockTtl())
	require.Equal(t, 1, calls)
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
	txnErr, ok := nokverrors.AsTxnKeyError(err)
	require.True(t, ok)
	require.Len(t, txnErr.Errors, 1)
	require.Equal(t, "resolve failed", txnErr.Errors[0].GetAbort())
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
	require.True(t, IsProtocolError(err))
	require.ErrorContains(t, err, fmt.Sprintf("primary key %q missing from mutations", []byte("omega")))
}

func TestClientTwoPhaseCommitRejectsMissingPrewritePayload(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
		},
		leaderStore: 1,
	})
	commitCalls := 0
	addr, stop := startBlockingStore(t, &scriptedKVService{
		mockService: mockService{storeID: 1, cluster: cluster},
		prewriteFn: func(context.Context, *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
			return &kvrpcpb.KvPrewriteResponse{}, nil
		},
		commitFn: func(context.Context, *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
			commitCalls++
			return &kvrpcpb.KvCommitResponse{Response: &kvrpcpb.CommitResponse{}}, nil
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

	err = cli.TwoPhaseCommit(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value")},
	}, 10, 11, 3000)
	require.True(t, IsProtocolError(err))
	require.ErrorContains(t, err, "missing prewrite payload")
	require.Zero(t, commitCalls)
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
		return svc.mockService.Prewrite(ctx, req)
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

func TestClientTwoPhaseCommitRollsBackPrewritesAfterPrimaryCommitTsExpired(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
		},
		leaderStore: 1,
	})
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
	svc.commitFn = func(context.Context, *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
		return &kvrpcpb.KvCommitResponse{
			Response: &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{CommitTsExpired: &kvrpcpb.CommitTsExpired{
					Key:         []byte("alfa"),
					CommitTs:    51,
					MinCommitTs: 55,
				}},
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
		{Op: kvrpcpb.Mutation_Put, Key: []byte("bravo"), Value: []byte("v2")},
	}, 50, 51, 3000)
	txnErr, ok := nokverrors.AsTxnKeyError(err)
	require.True(t, ok)
	require.Len(t, txnErr.Errors, 1)
	require.NotNil(t, txnErr.Errors[0].GetCommitTsExpired())

	cluster.mu.Lock()
	_, pending := cluster.regions[1].pending[50]
	rollbackHits := cluster.regions[1].rollbackHits
	cluster.mu.Unlock()
	require.False(t, pending, "prewrites must be rolled back after deterministic primary commit rejection")
	require.Equal(t, 1, rollbackHits)
}

func TestClientMutateWithCommitTimestampAllocatesAfterPrewrite(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
		},
		leaderStore: 1,
	})
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
	var prewrites int
	var commits int
	var allocs int
	svc.prewriteFn = func(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
		prewrites++
		return svc.mockService.Prewrite(ctx, req)
	}
	svc.commitFn = func(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
		require.Equal(t, 1, prewrites)
		require.Equal(t, 1, allocs)
		require.Equal(t, uint64(77), req.GetRequest().GetCommitVersion())
		commits++
		return svc.mockService.Commit(ctx, req)
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

	err = cli.MutateWithCommitTimestamp(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
	}, 50, 3000, func(context.Context) (uint64, error) {
		require.Equal(t, 1, prewrites)
		require.Equal(t, 0, commits)
		allocs++
		return 77, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, prewrites)
	require.Equal(t, 1, allocs)
	require.Equal(t, 1, commits)
}

func TestClientMutateWithCommitTimestampRollsBackAfterAllocationFailure(t *testing.T) {
	cluster := newMockCluster(clusterRegion{
		meta: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
		},
		leaderStore: 1,
	})
	svc := &scriptedKVService{mockService: mockService{storeID: 1, cluster: cluster}}
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

	allocErr := errors.New("tso unavailable")
	err = cli.MutateWithCommitTimestamp(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
	}, 50, 3000, func(context.Context) (uint64, error) {
		return 0, allocErr
	})
	require.ErrorIs(t, err, allocErr)

	cluster.mu.Lock()
	_, pending := cluster.regions[1].pending[50]
	rollbackHits := cluster.regions[1].rollbackHits
	cluster.mu.Unlock()
	require.False(t, pending, "prewrites must be rolled back when post-prewrite TSO allocation fails")
	require.Equal(t, 1, rollbackHits)
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
		return svc.mockService.Commit(ctx, req)
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
		return svc.mockService.Commit(ctx, req)
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
		return svc.mockService.Prewrite(ctx, req)
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
		return svc.mockService.Commit(ctx, req)
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
