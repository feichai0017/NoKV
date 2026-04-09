package client

import (
	"context"
	"errors"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

type clusterValue struct {
	value         []byte
	commitVersion uint64
}

type clusterPending struct {
	value  []byte
	delete bool
}

type clusterRegion struct {
	meta         *metapb.RegionDescriptor
	leaderStore  uint64
	pending      map[uint64]map[string]clusterPending // startVersion -> key
	committed    map[string]clusterValue
	prewriteHits int
	commitHits   int
	getHits      int
	scanHits     int
}

type mockCluster struct {
	mu             sync.Mutex
	regions        map[uint64]*clusterRegion
	notLeaderCount int32
}

func newMockCluster(regions ...clusterRegion) *mockCluster {
	mc := &mockCluster{
		regions: make(map[uint64]*clusterRegion, len(regions)),
	}
	for i := range regions {
		region := regions[i]
		if region.pending == nil {
			region.pending = make(map[uint64]map[string]clusterPending)
		}
		if region.committed == nil {
			region.committed = make(map[string]clusterValue)
		}
		mc.regions[region.meta.GetRegionId()] = &region
	}
	return mc
}

func (mc *mockCluster) regionMeta(id uint64) (*metapb.RegionDescriptor, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	region, ok := mc.regions[id]
	if !ok || region == nil {
		return nil, false
	}
	return protoClone(region.meta), true
}

type mockRegionResolver struct {
	mu       sync.Mutex
	region   *metapb.RegionDescriptor
	regions  []*metapb.RegionDescriptor
	err      error
	errs     []error
	calls    int
	closed   bool
	closeErr error
}

type blockingRegionResolver struct {
	started chan struct{}
}

type keyedBlockingResolver struct {
	started     chan struct{}
	blockedKeys map[string]struct{}
	regions     []*metapb.RegionDescriptor
}

func (mr *mockRegionResolver) GetRegionByKey(_ context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.calls++
	if len(mr.errs) > 0 {
		err := mr.errs[0]
		mr.errs = mr.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	if mr.err != nil {
		return nil, mr.err
	}
	if req == nil {
		return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
	}
	if len(mr.regions) > 0 {
		for _, meta := range mr.regions {
			if meta != nil && containsKey(metacodec.DescriptorFromProto(meta), req.GetKey()) {
				return routeResponse(meta), nil
			}
		}
		return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
	}
	if mr.region == nil || !containsKey(metacodec.DescriptorFromProto(mr.region), req.GetKey()) {
		return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
	}
	return routeResponse(mr.region), nil
}

func (mr *mockRegionResolver) Close() error {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.closed = true
	return mr.closeErr
}

func (br *blockingRegionResolver) GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	if br.started != nil {
		select {
		case br.started <- struct{}{}:
		default:
		}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (br *blockingRegionResolver) Close() error { return nil }

func (kr *keyedBlockingResolver) GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	if req != nil {
		if _, blocked := kr.blockedKeys[string(req.GetKey())]; blocked {
			if kr.started != nil {
				select {
				case kr.started <- struct{}{}:
				default:
				}
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		for _, meta := range kr.regions {
			if meta != nil && containsKey(metacodec.DescriptorFromProto(meta), req.GetKey()) {
				return routeResponse(meta), nil
			}
		}
	}
	return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
}

func (kr *keyedBlockingResolver) Close() error { return nil }

func resolverFromCluster(cluster *mockCluster) *mockRegionResolver {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	regions := make([]*metapb.RegionDescriptor, 0, len(cluster.regions))
	for _, region := range cluster.regions {
		if region == nil || region.meta == nil {
			continue
		}
		regions = append(regions, protoClone(region.meta))
	}
	sort.Slice(regions, func(i, j int) bool {
		return regions[i].GetRegionId() < regions[j].GetRegionId()
	})
	return &mockRegionResolver{regions: regions}
}

func (mc *mockCluster) prewrite(storeID uint64, regionID uint64, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, *errorpb.RegionError) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	region, ok := mc.regions[regionID]
	if !ok || region == nil {
		return nil, epochNotMatch(mc.regions)
	}
	if storeID != region.leaderStore {
		atomic.AddInt32(&mc.notLeaderCount, 1)
		return nil, notLeaderError(region)
	}
	if req == nil {
		return &kvrpcpb.PrewriteResponse{}, nil
	}
	pending := region.pending[req.GetStartVersion()]
	if pending == nil {
		pending = make(map[string]clusterPending)
		region.pending[req.GetStartVersion()] = pending
	}
	for _, mut := range req.GetMutations() {
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put:
			pending[string(mut.GetKey())] = clusterPending{value: append([]byte(nil), mut.GetValue()...)}
		case kvrpcpb.Mutation_Delete:
			pending[string(mut.GetKey())] = clusterPending{delete: true}
		default:
			return &kvrpcpb.PrewriteResponse{
				Errors: []*kvrpcpb.KeyError{{Abort: "unsupported mutation"}},
			}, nil
		}
	}
	region.prewriteHits++
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (mc *mockCluster) commit(storeID uint64, regionID uint64, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, *errorpb.RegionError) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	region, ok := mc.regions[regionID]
	if !ok || region == nil {
		atomic.AddInt32(&mc.notLeaderCount, 1)
		return nil, epochNotMatch(mc.regions)
	}
	if storeID != region.leaderStore {
		atomic.AddInt32(&mc.notLeaderCount, 1)
		return nil, notLeaderError(region)
	}
	if req == nil {
		return &kvrpcpb.CommitResponse{}, nil
	}
	pending := region.pending[req.GetStartVersion()]
	if pending == nil {
		return &kvrpcpb.CommitResponse{}, nil
	}
	for _, key := range req.GetKeys() {
		pend, ok := pending[string(key)]
		if !ok {
			continue
		}
		if pend.delete {
			delete(region.committed, string(key))
		} else {
			region.committed[string(key)] = clusterValue{
				value:         append([]byte(nil), pend.value...),
				commitVersion: req.GetCommitVersion(),
			}
		}
		delete(pending, string(key))
	}
	if len(pending) == 0 {
		delete(region.pending, req.GetStartVersion())
	}
	region.commitHits++
	return &kvrpcpb.CommitResponse{}, nil
}

func (mc *mockCluster) get(storeID uint64, req *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	ctx := req.GetContext()
	if ctx == nil {
		return nil, statusInvalidArgument("context missing")
	}
	regionID := ctx.GetRegionId()
	mc.mu.Lock()
	defer mc.mu.Unlock()
	region, ok := mc.regions[regionID]
	if !ok || region == nil {
		return nil, statusInvalidArgument("region not found")
	}
	if storeID != region.leaderStore {
		atomic.AddInt32(&mc.notLeaderCount, 1)
		return &kvrpcpb.KvGetResponse{RegionError: notLeaderError(region)}, nil
	}
	if req.GetRequest() == nil {
		return &kvrpcpb.KvGetResponse{}, nil
	}
	key := req.GetRequest().GetKey()
	version := req.GetRequest().GetVersion()
	region.getHits++
	val, ok := region.committed[string(key)]
	if !ok || val.commitVersion > version {
		return &kvrpcpb.KvGetResponse{Response: &kvrpcpb.GetResponse{NotFound: true}}, nil
	}
	return &kvrpcpb.KvGetResponse{
		Response: &kvrpcpb.GetResponse{Value: append([]byte(nil), val.value...)},
	}, nil
}

func (mc *mockCluster) scan(storeID uint64, req *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
	ctx := req.GetContext()
	if ctx == nil {
		return nil, statusInvalidArgument("context missing")
	}
	regionID := ctx.GetRegionId()
	mc.mu.Lock()
	defer mc.mu.Unlock()
	region, ok := mc.regions[regionID]
	if !ok || region == nil {
		return nil, statusInvalidArgument("region not found")
	}
	if storeID != region.leaderStore {
		atomic.AddInt32(&mc.notLeaderCount, 1)
		return &kvrpcpb.KvScanResponse{RegionError: notLeaderError(region)}, nil
	}
	scanReq := req.GetRequest()
	if scanReq == nil {
		return &kvrpcpb.KvScanResponse{Response: &kvrpcpb.ScanResponse{}}, nil
	}
	startKey := scanReq.GetStartKey()
	version := scanReq.GetVersion()
	limit := scanReq.GetLimit()
	if limit == 0 {
		limit = 1
	}
	keys := make([]string, 0, len(region.committed))
	for key, val := range region.committed {
		if val.commitVersion <= version {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	resp := &kvrpcpb.ScanResponse{}
	for _, key := range keys {
		if len(resp.Kvs) >= int(limit) {
			break
		}
		if len(startKey) > 0 && key < string(startKey) {
			continue
		}
		val := region.committed[key]
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KV{
			Key:   []byte(key),
			Value: append([]byte(nil), val.value...),
		})
	}
	region.scanHits++
	return &kvrpcpb.KvScanResponse{Response: resp}, nil
}

type mockService struct {
	storeID uint64
	cluster *mockCluster
}

type blockingService struct {
	kvrpcpb.UnimplementedNoKVServer
	started chan struct{}
}

type regionBlockingService struct {
	mockService
	started            chan struct{}
	blockPrewriteOn    uint64
	blockResolveLockOn uint64
}

func (s *blockingService) signal() {
	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
}

func (s *regionBlockingService) signal() {
	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
}

func (s *mockService) KvGet(ctx context.Context, req *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	return s.cluster.get(s.storeID, req)
}

func (s *mockService) KvBatchGet(ctx context.Context, req *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error) {
	if req == nil || req.GetContext() == nil {
		return nil, statusInvalidArgument("context required")
	}
	batch := req.GetRequest()
	if batch == nil || len(batch.GetRequests()) == 0 {
		return &kvrpcpb.KvBatchGetResponse{
			Response: &kvrpcpb.BatchGetResponse{},
		}, nil
	}
	responses := make([]*kvrpcpb.GetResponse, 0, len(batch.GetRequests()))
	for _, getReq := range batch.GetRequests() {
		if getReq == nil {
			responses = append(responses, &kvrpcpb.GetResponse{NotFound: true})
			continue
		}
		resp, err := s.cluster.get(s.storeID, &kvrpcpb.KvGetRequest{
			Context: req.GetContext(),
			Request: getReq,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return &kvrpcpb.KvBatchGetResponse{RegionError: resp.GetRegionError()}, nil
		}
		if resp.GetResponse() != nil {
			responses = append(responses, resp.GetResponse())
		} else {
			responses = append(responses, &kvrpcpb.GetResponse{NotFound: true})
		}
	}
	return &kvrpcpb.KvBatchGetResponse{
		Response: &kvrpcpb.BatchGetResponse{Responses: responses},
	}, nil
}

func (s *mockService) KvScan(ctx context.Context, req *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
	return s.cluster.scan(s.storeID, req)
}

func (s *mockService) KvPrewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	if req == nil || req.GetContext() == nil {
		return nil, statusInvalidArgument("context required")
	}
	resp, regionErr := s.cluster.prewrite(s.storeID, req.GetContext().GetRegionId(), req.GetRequest())
	return &kvrpcpb.KvPrewriteResponse{
		Response:    resp,
		RegionError: regionErr,
	}, nil
}

func (s *mockService) KvCommit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	if req == nil || req.GetContext() == nil {
		return nil, statusInvalidArgument("context required")
	}
	resp, regionErr := s.cluster.commit(s.storeID, req.GetContext().GetRegionId(), req.GetRequest())
	return &kvrpcpb.KvCommitResponse{
		Response:    resp,
		RegionError: regionErr,
	}, nil
}

func (s *mockService) KvBatchRollback(context.Context, *kvrpcpb.KvBatchRollbackRequest) (*kvrpcpb.KvBatchRollbackResponse, error) {
	return &kvrpcpb.KvBatchRollbackResponse{}, nil
}

func (s *mockService) KvResolveLock(context.Context, *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
	return &kvrpcpb.KvResolveLockResponse{}, nil
}

func (s *mockService) KvCheckTxnStatus(context.Context, *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
	return &kvrpcpb.KvCheckTxnStatusResponse{}, nil
}

func (s *blockingService) KvGet(ctx context.Context, req *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	s.signal()
	<-ctx.Done()
	return nil, status.Error(codes.Canceled, ctx.Err().Error())
}

func (s *blockingService) KvPrewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	s.signal()
	<-ctx.Done()
	return nil, status.Error(codes.Canceled, ctx.Err().Error())
}

func (s *blockingService) KvCommit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	s.signal()
	<-ctx.Done()
	return nil, status.Error(codes.Canceled, ctx.Err().Error())
}

func (s *regionBlockingService) KvPrewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	if req != nil && req.GetContext() != nil && req.GetContext().GetRegionId() == s.blockPrewriteOn {
		s.signal()
		<-ctx.Done()
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
	return s.mockService.KvPrewrite(ctx, req)
}

func (s *regionBlockingService) KvResolveLock(ctx context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
	if req != nil && req.GetContext() != nil && req.GetContext().GetRegionId() == s.blockResolveLockOn {
		s.signal()
		<-ctx.Done()
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
	return s.mockService.KvResolveLock(ctx, req)
}

func startMockStore(t *testing.T, cluster *mockCluster, storeID uint64) (string, func()) {
	t.Helper()
	srv := grpc.NewServer()
	service := &mockService{storeID: storeID, cluster: cluster}
	kvrpcpb.RegisterNoKVServer(srv, service)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func startBlockingStore(t *testing.T, service kvrpcpb.NoKVServer) (string, func()) {
	t.Helper()
	srv := grpc.NewServer()
	kvrpcpb.RegisterNoKVServer(srv, service)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func TestClientTwoPhaseCommitAndGet(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 101},
					{StoreId: 2, PeerId: 201},
				},
			},
			leaderStore: 1,
		},
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 102},
					{StoreId: 2, PeerId: 202},
				},
			},
			leaderStore: 1,
		},
	)

	addrLeader, stopLeader := startMockStore(t, cluster, 1)
	defer stopLeader()
	addrFollower, stopFollower := startMockStore(t, cluster, 2)
	defer stopFollower()

	clientCfg := Config{
		Stores: []StoreEndpoint{
			{StoreID: 2, Addr: addrFollower},
			{StoreID: 1, Addr: addrLeader},
		},
		RegionResolver: func() *mockRegionResolver {
			resolver := resolverFromCluster(cluster)
			// Force an initial stale leader guess so NotLeader retry path is
			// exercised under Coordinator-resolver mode as well.
			for _, meta := range resolver.regions {
				if meta == nil || len(meta.GetPeers()) < 2 {
					continue
				}
				for i, p := range meta.GetPeers() {
					if p != nil && p.GetStoreId() == 2 {
						meta.Peers[0], meta.Peers[i] = meta.Peers[i], meta.Peers[0]
						break
					}
				}
			}
			return resolver
		}(),
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	cli, err := New(clientCfg)
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mutations := []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value-a")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("zenith"), Value: []byte("value-z")},
	}
	err = cli.TwoPhaseCommit(ctx, []byte("alfa"), mutations, 100, 150, 3000)
	require.NoError(t, err)

	getResp, err := cli.Get(context.Background(), []byte("alfa"), 200)
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, []byte("value-a"), getResp.GetValue())

	scan, err := cli.Scan(context.Background(), []byte("a"), 4, 200)
	require.NoError(t, err)
	require.Len(t, scan, 2)
	require.Equal(t, []byte("alfa"), scan[0].GetKey())
	require.Equal(t, []byte("zenith"), scan[1].GetKey())

	require.GreaterOrEqual(t, atomic.LoadInt32(&cluster.notLeaderCount), int32(2), "client should retry after NotLeader")
	snap, ok := cluster.regionMeta(1)
	require.True(t, ok)
	require.NotNil(t, snap)
}

func TestClientBatchGetAndMutateHelpers(t *testing.T) {
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
				EndKey:   nil,
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

	clientCfg := Config{
		Stores: []StoreEndpoint{
			{StoreID: 1, Addr: addr},
		},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	cli, err := New(clientCfg)
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, cli.Put(ctx, []byte("alfa"), []byte("value-a"), 10, 20, 3000))
	require.NoError(t, cli.Put(ctx, []byte("zulu"), []byte("value-z"), 11, 21, 3000))

	got, err := cli.BatchGet(ctx, [][]byte{[]byte("alfa"), []byte("zulu")}, 50)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), got["alfa"].GetValue())
	require.Equal(t, []byte("value-z"), got["zulu"].GetValue())

	require.NoError(t, cli.Delete(ctx, []byte("alfa"), 60, 61, 3000))
	got, err = cli.BatchGet(ctx, [][]byte{[]byte("alfa")}, 70)
	require.NoError(t, err)
	require.True(t, got["alfa"].GetNotFound())

	err = cli.Mutate(ctx, []byte("primary"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("other"), Value: []byte("v")},
	}, 1, 2, 3000)
	require.Error(t, err)

	require.NoError(t, cli.Mutate(ctx, []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v2")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("bravo"), Value: []byte("v3")},
	}, 80, 81, 3000))

	resp, err := cli.CheckTxnStatus(ctx, []byte("alfa"), 1, 2)
	require.NoError(t, err)
	require.Nil(t, resp)

	resolved, err := cli.ResolveLocks(ctx, 1, 0, [][]byte{[]byte("alfa"), []byte("zulu")})
	require.NoError(t, err)
	require.Equal(t, uint64(0), resolved)

	errStr := (&KeyConflictError{Errors: []*kvrpcpb.KeyError{{Abort: "boom"}}}).Error()
	require.Contains(t, errStr, "client: prewrite key errors")
}

func TestNewRequiresRegionResolver(t *testing.T) {
	_, err := New(Config{
		Stores: []StoreEndpoint{
			{StoreID: 1, Addr: "127.0.0.1:1"},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "region resolver required")
}

func TestClientRegionResolverLookupAndCache(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 101},
				},
			},
			leaderStore: 1,
			committed: map[string]clusterValue{
				"alfa": {value: []byte("value-a"), commitVersion: 10},
			},
		},
	)
	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	resolver := &mockRegionResolver{region: cluster.regions[1].meta}
	cli, err := New(Config{
		Stores: []StoreEndpoint{
			{StoreID: 1, Addr: addr},
		},
		RegionResolver: resolver,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)

	resp, err := cli.Get(context.Background(), []byte("alfa"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), resp.GetValue())

	resp, err = cli.Get(context.Background(), []byte("alfa"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), resp.GetValue())

	resolver.mu.Lock()
	require.Equal(t, 1, resolver.calls, "second lookup should hit local region cache")
	resolver.mu.Unlock()

	require.NoError(t, cli.Close())
	resolver.mu.Lock()
	require.True(t, resolver.closed)
	resolver.mu.Unlock()
}

type errorService struct {
	kvrpcpb.UnimplementedNoKVServer
}

func (s *errorService) KvGet(context.Context, *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	return nil, status.Error(codes.Unavailable, "boom")
}

func startErrorStore(t *testing.T) (string, func()) {
	t.Helper()
	srv := grpc.NewServer()
	kvrpcpb.RegisterNoKVServer(srv, &errorService{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

type flakyGetService struct {
	kvrpcpb.UnimplementedNoKVServer
	mu       sync.Mutex
	failures int
	resp     *kvrpcpb.KvGetResponse
}

func (s *flakyGetService) KvGet(context.Context, *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures > 0 {
		s.failures--
		return nil, status.Error(codes.Unavailable, "boom")
	}
	return proto.Clone(s.resp).(*kvrpcpb.KvGetResponse), nil
}

func startFlakyGetStore(t *testing.T, failures int, resp *kvrpcpb.KvGetResponse) (string, func()) {
	t.Helper()
	srv := grpc.NewServer()
	kvrpcpb.RegisterNoKVServer(srv, &flakyGetService{failures: failures, resp: resp})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func TestNormalizeRPCErrorOnGet(t *testing.T) {
	addr, stop := startErrorStore(t)
	defer stop()

	meta := &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: nil,
		EndKey:   nil,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
		},
	}
	cli, err := New(Config{
		Stores: []StoreEndpoint{
			{StoreID: 1, Addr: addr},
		},
		RegionResolver: &mockRegionResolver{region: meta},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	_, err = cli.Get(context.Background(), []byte("key"), 1)
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestClientRegionResolverLookupErrors(t *testing.T) {
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
	)
	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	makeClient := func(resolver *mockRegionResolver) *Client {
		cli, err := New(Config{
			Stores: []StoreEndpoint{
				{StoreID: 1, Addr: addr},
			},
			RegionResolver: resolver,
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cli.Close() })
		return cli
	}

	t.Run("not found", func(t *testing.T) {
		cli := makeClient(&mockRegionResolver{})
		_, err := cli.Get(context.Background(), []byte("zulu"), 1)
		require.Error(t, err)
		var notFound *RegionNotFoundError
		require.ErrorAs(t, err, &notFound)
		require.True(t, IsRegionNotFound(err))
		require.False(t, IsRouteUnavailable(err))
	})

	t.Run("resolver unavailable", func(t *testing.T) {
		cli := makeClient(&mockRegionResolver{err: status.Error(codes.Unavailable, "pd down")})
		_, err := cli.Get(context.Background(), []byte("alfa"), 1)
		require.Error(t, err)
		var routeErr *RouteUnavailableError
		require.ErrorAs(t, err, &routeErr)
		require.True(t, IsRouteUnavailable(err))
		require.False(t, IsRegionNotFound(err))
		require.Equal(t, codes.Unavailable, status.Code(routeErr.Err))
	})
}

func TestClientRetriesRouteUnavailable(t *testing.T) {
	meta := &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
		},
	}
	addr, stop := startFlakyGetStore(t, 0, &kvrpcpb.KvGetResponse{
		Response: &kvrpcpb.GetResponse{Value: []byte("value-a")},
	})
	defer stop()

	resolver := &mockRegionResolver{
		region: meta,
		errs:   []error{status.Error(codes.Unavailable, "pd down")},
	}
	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolver,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		Retry: RetryPolicy{
			MaxAttempts:             3,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resp, err := cli.Get(context.Background(), []byte("alfa"), 1)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), resp.GetValue())

	resolver.mu.Lock()
	require.Equal(t, 2, resolver.calls)
	resolver.mu.Unlock()
}

func TestClientRetriesTransportUnavailable(t *testing.T) {
	meta := &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
		},
	}
	addr, stop := startFlakyGetStore(t, 1, &kvrpcpb.KvGetResponse{
		Response: &kvrpcpb.GetResponse{Value: []byte("value-a")},
	})
	defer stop()

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: &mockRegionResolver{region: meta},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		Retry: RetryPolicy{
			MaxAttempts:                 3,
			TransportUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resp, err := cli.Get(context.Background(), []byte("alfa"), 1)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), resp.GetValue())
}

func TestClientTwoPhaseCommitRetriesRouteUnavailableDuringGrouping(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 101},
				},
			},
			leaderStore: 1,
		},
	)

	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	resolver := resolverFromCluster(cluster)
	resolver.errs = []error{status.Error(codes.Unavailable, "pd down")}

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolver,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		Retry: RetryPolicy{
			MaxAttempts:             3,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	err = cli.TwoPhaseCommit(context.Background(), []byte("alfa"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value-a")},
	}, 10, 20, 3000)
	require.NoError(t, err)

	resolver.mu.Lock()
	require.GreaterOrEqual(t, resolver.calls, 2)
	resolver.mu.Unlock()
}

func TestClientResolveLocksRetriesRouteUnavailableDuringGrouping(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 101},
				},
			},
			leaderStore: 1,
		},
	)

	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	resolver := resolverFromCluster(cluster)
	resolver.errs = []error{status.Error(codes.Unavailable, "pd down")}

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolver,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		Retry: RetryPolicy{
			MaxAttempts:             3,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resolved, err := cli.ResolveLocks(context.Background(), 1, 0, [][]byte{[]byte("alfa")})
	require.NoError(t, err)
	require.Equal(t, uint64(0), resolved)

	resolver.mu.Lock()
	require.GreaterOrEqual(t, resolver.calls, 2)
	resolver.mu.Unlock()
}

func TestClientCheckTxnStatusRetriesRouteUnavailable(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &metapb.RegionDescriptor{
				RegionId: 1,
				StartKey: []byte("a"),
				EndKey:   nil,
				Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 101},
				},
			},
			leaderStore: 1,
		},
	)

	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	resolver := resolverFromCluster(cluster)
	resolver.errs = []error{status.Error(codes.Unavailable, "pd down")}

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolver,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		Retry: RetryPolicy{
			MaxAttempts:             3,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	_, err = cli.CheckTxnStatus(context.Background(), []byte("alfa"), 1, 2)
	require.NoError(t, err)

	resolver.mu.Lock()
	require.GreaterOrEqual(t, resolver.calls, 2)
	resolver.mu.Unlock()
}

func TestClientLazyDialSkipsUnusedStoreEndpoints(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
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
			committed: map[string]clusterValue{
				"alfa": {value: []byte("value-a"), commitVersion: 10},
			},
		},
	)
	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	cli, err := New(Config{
		Stores: []StoreEndpoint{
			{StoreID: 1, Addr: addr},
			{StoreID: 2, Addr: "127.0.0.1:1"},
		},
		RegionResolver: &mockRegionResolver{region: cluster.regions[1].meta},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resp, err := cli.Get(context.Background(), []byte("alfa"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), resp.GetValue())

	cli.mu.RLock()
	require.NotNil(t, cli.stores[1].conn)
	require.Nil(t, cli.stores[2].conn, "unused store should not be dialed eagerly")
	cli.mu.RUnlock()
}

func TestClientRegionResolverLookupUsesIndexedCacheAcrossRegions(t *testing.T) {
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
			committed: map[string]clusterValue{
				"alfa": {value: []byte("value-a"), commitVersion: 10},
			},
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
			committed: map[string]clusterValue{
				"omega": {value: []byte("value-z"), commitVersion: 10},
			},
		},
	)
	addr, stop := startMockStore(t, cluster, 1)
	defer stop()

	resolver := resolverFromCluster(cluster)
	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolver,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	resp, err := cli.Get(context.Background(), []byte("alfa"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), resp.GetValue())

	resp, err = cli.Get(context.Background(), []byte("omega"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value-z"), resp.GetValue())

	resp, err = cli.Get(context.Background(), []byte("alfa"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value-a"), resp.GetValue())

	resolver.mu.Lock()
	require.Equal(t, 2, resolver.calls, "subsequent indexed cache lookup should not hit resolver")
	resolver.mu.Unlock()
	require.Len(t, cli.regionIndex, 2)
	require.Equal(t, uint64(1), cli.regionIndex[0].regionID)
	require.Equal(t, uint64(2), cli.regionIndex[1].regionID)
}

func TestClientHandleRegionErrorUpdatesIndexedCache(t *testing.T) {
	cli := &Client{
		regions: make(map[uint64]*regionState),
	}
	cli.upsertRegionLocked(metacodec.DescriptorFromProto(&metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
		},
	}), 1)

	err := cli.handleRegionError(1, &errorpb.RegionError{
		EpochNotMatch: &errorpb.EpochNotMatch{
			Regions: []*metapb.RegionDescriptor{
				{
					RegionId: 11,
					StartKey: []byte("a"),
					EndKey:   []byte("m"),
					Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 1},
					Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 111}},
				},
				{
					RegionId: 12,
					StartKey: []byte("m"),
					EndKey:   []byte("z"),
					Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 1},
					Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 121}},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotContains(t, cli.regions, uint64(1))
	require.Contains(t, cli.regions, uint64(11))
	require.Contains(t, cli.regions, uint64(12))
	require.Len(t, cli.regionIndex, 2)

	got, ok := cli.regionForKeyFromCache([]byte("beta"))
	require.True(t, ok)
	require.Equal(t, uint64(11), got.desc.RegionID)

	got, ok = cli.regionForKeyFromCache([]byte("omega"))
	require.True(t, ok)
	require.Equal(t, uint64(12), got.desc.RegionID)
}

// Utility helpers

func routeResponse(meta *metapb.RegionDescriptor) *coordpb.GetRegionByKeyResponse {
	if meta == nil {
		return &coordpb.GetRegionByKeyResponse{NotFound: true}
	}
	return &coordpb.GetRegionByKeyResponse{
		RegionDescriptor: metacodec.DescriptorToProto(metacodec.DescriptorFromProto(meta)),
	}
}

func protoClone(meta *metapb.RegionDescriptor) *metapb.RegionDescriptor {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*metapb.RegionDescriptor)
}

func protoClonePeer(peer *metapb.RegionPeer) *metapb.RegionPeer {
	if peer == nil {
		return nil
	}
	return proto.Clone(peer).(*metapb.RegionPeer)
}

func notLeaderError(region *clusterRegion) *errorpb.RegionError {
	return &errorpb.RegionError{
		NotLeader: &errorpb.NotLeader{
			RegionId: region.meta.GetRegionId(),
			Leader:   leaderPeer(region.meta, region.leaderStore),
		},
	}
}

func leaderPeer(meta *metapb.RegionDescriptor, storeID uint64) *metapb.RegionPeer {
	for _, peer := range meta.GetPeers() {
		if peer.GetStoreId() == storeID {
			return protoClonePeer(peer)
		}
	}
	return nil
}

func epochNotMatch(regions map[uint64]*clusterRegion) *errorpb.RegionError {
	resp := &errorpb.RegionError{
		EpochNotMatch: &errorpb.EpochNotMatch{},
	}
	for _, region := range regions {
		resp.EpochNotMatch.Regions = append(resp.EpochNotMatch.Regions, protoClone(region.meta))
	}
	return resp
}

func statusInvalidArgument(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}

func TestContainsKeyAndCompare(t *testing.T) {
	require.False(t, containsKey(descriptor.Descriptor{}, []byte("a")))

	meta := &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: []byte("b"),
		EndKey:   []byte("d"),
	}
	desc := metacodec.DescriptorFromProto(meta)
	require.False(t, containsKey(desc, []byte("a")))
	require.True(t, containsKey(desc, []byte("b")))
	require.True(t, containsKey(desc, []byte("c")))
	require.False(t, containsKey(desc, []byte("d")))

	require.Equal(t, 0, bytesCompare([]byte("a"), []byte("a")))
	require.Equal(t, -1, bytesCompare([]byte("a"), []byte("b")))
	require.Equal(t, 1, bytesCompare([]byte("b"), []byte("a")))
	require.Equal(t, -1, bytesCompare([]byte("a"), []byte("aa")))
	require.Equal(t, 1, bytesCompare([]byte("aa"), []byte("a")))
}

func TestIncrementKey(t *testing.T) {
	require.Equal(t, []byte("ab\x01"), incrementKey([]byte("ab\x00")))
	require.Equal(t, []byte{0x01, 0x00}, incrementKey([]byte{0x00, 0xff}))
	require.Equal(t, []byte{0x00, 0x00, 0x00}, incrementKey([]byte{0xff, 0xff}))
}

func TestCloneHelpers(t *testing.T) {
	meta := &metapb.RegionDescriptor{StartKey: []byte("a"), EndKey: []byte("z")}
	clone := protoClone(meta)
	require.NotSame(t, meta, clone)
	meta.StartKey[0] = 'b'
	require.Equal(t, []byte("a"), clone.StartKey)

	mut := &kvrpcpb.Mutation{Key: []byte("k"), Value: []byte("v")}
	mutClone := cloneMutation(mut)
	require.NotSame(t, mut, mutClone)
	mut.Key[0] = 'x'
	require.Equal(t, []byte("k"), mutClone.Key)

	require.Nil(t, protoClone(nil))
	require.Nil(t, cloneMutation(nil))
}

func TestCollectKeysAndPrimary(t *testing.T) {
	muts := []*kvrpcpb.Mutation{
		{Key: []byte("a")},
		nil,
		{Key: []byte("b")},
	}
	keys := collectKeys(muts)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, keys)

	require.True(t, mutationHasPrimary(muts, []byte("a")))
	require.False(t, mutationHasPrimary(muts, []byte("c")))

	clone := cloneKeys(keys)
	require.Equal(t, keys, clone)
	keys[0][0] = 'x'
	require.Equal(t, []byte("a"), clone[0])
}

func TestNormalizeRPCError(t *testing.T) {
	require.NoError(t, normalizeRPCError(nil))
}

func TestDefaultLeaderStoreID(t *testing.T) {
	require.Equal(t, uint64(0), defaultLeaderStoreID(descriptor.Descriptor{}))
	require.Equal(t, uint64(0), defaultLeaderStoreID(metacodec.DescriptorFromProto(&metapb.RegionDescriptor{})))
	require.Equal(t, uint64(9), defaultLeaderStoreID(metacodec.DescriptorFromProto(&metapb.RegionDescriptor{
		RegionId: 1,
		Peers: []*metapb.RegionPeer{
			nil,
			{StoreId: 9, PeerId: 90},
		},
	})))
}

func TestRegionForKeyFromResolverDropsStaleCachedLeader(t *testing.T) {
	resolver := &mockRegionResolver{
		region: &metapb.RegionDescriptor{
			RegionId: 1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 2},
			Peers: []*metapb.RegionPeer{
				{StoreId: 2, PeerId: 201},
				{StoreId: 3, PeerId: 301},
			},
		},
	}
	cli, err := New(Config{
		Stores: []StoreEndpoint{
			{StoreID: 2, Addr: "127.0.0.1:2"},
			{StoreID: 3, Addr: "127.0.0.1:3"},
		},
		RegionResolver: resolver,
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	cli.mu.Lock()
	cli.upsertRegionLocked(metacodec.DescriptorFromProto(&metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
			{StoreId: 2, PeerId: 201},
		},
	}), 1)
	cli.mu.Unlock()

	region, err := cli.regionForKeyFromResolver(context.Background(), []byte("m"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), region.leader)

	cached, ok := cli.regionSnapshot(1)
	require.True(t, ok)
	require.Equal(t, uint64(2), cached.leader)
}

func TestClientGetHonorsCanceledContextDuringRouteLookup(t *testing.T) {
	resolver := &blockingRegionResolver{started: make(chan struct{}, 1)}
	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: "127.0.0.1:1"}},
		RegionResolver: resolver,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:             1,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := cli.Get(ctx, []byte("alpha"), 10)
		done <- err
	}()

	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("resolver was not invoked")
	}
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, IsRouteUnavailable(err))
		require.True(t, errors.Is(err, context.Canceled))
	case <-time.After(time.Second):
		t.Fatal("client get did not return after context cancellation")
	}
}

func TestClientGetHonorsCanceledContextDuringRPC(t *testing.T) {
	meta := &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: []byte("a"),
		EndKey:   nil,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
	}
	service := &blockingService{started: make(chan struct{}, 1)}
	addr, stop := startBlockingStore(t, service)
	defer stop()

	resolver := &mockRegionResolver{region: meta}
	cli, err := New(Config{
		Stores:             []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver:     resolver,
		RouteLookupTimeout: time.Second,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:                 1,
			RouteUnavailableBackoff:     0,
			TransportUnavailableBackoff: 0,
			RegionErrorBackoff:          0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := cli.Get(ctx, []byte("alpha"), 10)
		done <- err
	}()

	require.Eventually(t, func() bool {
		select {
		case <-service.started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("client rpc did not return after context cancellation")
	}
}

func TestClientPutHonorsCanceledContextDuringRouteLookup(t *testing.T) {
	resolver := &blockingRegionResolver{started: make(chan struct{}, 1)}
	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: "127.0.0.1:1"}},
		RegionResolver: resolver,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:             1,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cli.Put(ctx, []byte("alpha"), []byte("beta"), 10, 11, 3000)
	}()

	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("resolver was not invoked")
	}
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, IsRouteUnavailable(err))
		require.True(t, errors.Is(err, context.Canceled))
	case <-time.After(time.Second):
		t.Fatal("client put did not return after context cancellation")
	}
}

func TestClientPutHonorsCanceledContextDuringRPC(t *testing.T) {
	meta := &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: []byte("a"),
		EndKey:   nil,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
	}
	service := &blockingService{started: make(chan struct{}, 1)}
	addr, stop := startBlockingStore(t, service)
	defer stop()

	resolver := &mockRegionResolver{region: meta}
	cli, err := New(Config{
		Stores:             []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver:     resolver,
		RouteLookupTimeout: time.Second,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:                 1,
			RouteUnavailableBackoff:     0,
			TransportUnavailableBackoff: 0,
			RegionErrorBackoff:          0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cli.Put(ctx, []byte("alpha"), []byte("beta"), 10, 11, 3000)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-service.started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("client put rpc did not return after context cancellation")
	}
}

func TestClientTwoPhaseCommitHonorsCanceledContextDuringMultiRegionRouteLookup(t *testing.T) {
	metaA := &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 101}},
	}
	metaB := &metapb.RegionDescriptor{
		RegionId: 2,
		StartKey: []byte("m"),
		EndKey:   nil,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 201}},
	}
	resolver := &keyedBlockingResolver{
		started:     make(chan struct{}, 1),
		blockedKeys: map[string]struct{}{"omega": {}},
		regions:     []*metapb.RegionDescriptor{metaA, metaB},
	}
	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: "127.0.0.1:1"}},
		RegionResolver: resolver,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:             1,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cli.TwoPhaseCommit(ctx, []byte("alfa"), []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
			{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
		}, 10, 11, 3000)
	}()

	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("resolver was not invoked for second region")
	}
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, IsRouteUnavailable(err))
		require.True(t, errors.Is(err, context.Canceled))
	case <-time.After(time.Second):
		t.Fatal("two-phase commit did not return after context cancellation")
	}
}

func TestClientTwoPhaseCommitHonorsCanceledContextDuringMultiRegionRPC(t *testing.T) {
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
	service := &regionBlockingService{
		mockService:        mockService{storeID: 1, cluster: cluster},
		started:            make(chan struct{}, 1),
		blockPrewriteOn:    2,
		blockResolveLockOn: 0,
	}
	addr, stop := startBlockingStore(t, service)
	defer stop()

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:                 1,
			RouteUnavailableBackoff:     0,
			TransportUnavailableBackoff: 0,
			RegionErrorBackoff:          0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cli.TwoPhaseCommit(ctx, []byte("alfa"), []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("v1")},
			{Op: kvrpcpb.Mutation_Put, Key: []byte("omega"), Value: []byte("v2")},
		}, 20, 21, 3000)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-service.started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("two-phase commit rpc did not return after context cancellation")
	}
}

func TestClientResolveLocksHonorsCanceledContextDuringMultiRegionRPC(t *testing.T) {
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
	service := &regionBlockingService{
		mockService:        mockService{storeID: 1, cluster: cluster},
		started:            make(chan struct{}, 1),
		blockResolveLockOn: 2,
	}
	addr, stop := startBlockingStore(t, service)
	defer stop()

	cli, err := New(Config{
		Stores:         []StoreEndpoint{{StoreID: 1, Addr: addr}},
		RegionResolver: resolverFromCluster(cluster),
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: RetryPolicy{
			MaxAttempts:                 1,
			RouteUnavailableBackoff:     0,
			TransportUnavailableBackoff: 0,
			RegionErrorBackoff:          0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := cli.ResolveLocks(ctx, 30, 0, [][]byte{[]byte("alfa"), []byte("omega")})
		done <- err
	}()

	require.Eventually(t, func() bool {
		select {
		case <-service.started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("resolve locks rpc did not return after context cancellation")
	}
}
