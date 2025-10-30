package client_test

import (
	"context"
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

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/client"
)

type clusterValue struct {
	value         []byte
	commitVersion uint64
}

type clusterPending struct {
	value []byte
}

type clusterRegion struct {
	meta         *pb.RegionMeta
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
		mc.regions[region.meta.GetId()] = &region
	}
	return mc
}

func (mc *mockCluster) regionMeta(id uint64) (*pb.RegionMeta, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	region, ok := mc.regions[id]
	if !ok || region == nil {
		return nil, false
	}
	return protoClone(region.meta), true
}

func (mc *mockCluster) leaderPeer(regionID uint64) *pb.RegionPeer {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	region, ok := mc.regions[regionID]
	if !ok || region == nil {
		return nil
	}
	for _, peer := range region.meta.GetPeers() {
		if peer.GetStoreId() == region.leaderStore {
			return protoClonePeer(peer)
		}
	}
	return nil
}

func (mc *mockCluster) prewrite(storeID uint64, regionID uint64, req *pb.PrewriteRequest) (*pb.PrewriteResponse, *pb.RegionError) {
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
		return &pb.PrewriteResponse{}, nil
	}
	pending := region.pending[req.GetStartVersion()]
	if pending == nil {
		pending = make(map[string]clusterPending)
		region.pending[req.GetStartVersion()] = pending
	}
	for _, mut := range req.GetMutations() {
		switch mut.GetOp() {
		case pb.Mutation_Put:
			pending[string(mut.GetKey())] = clusterPending{value: append([]byte(nil), mut.GetValue()...)}
		default:
			return &pb.PrewriteResponse{
				Errors: []*pb.KeyError{{Abort: "unsupported mutation"}},
			}, nil
		}
	}
	region.prewriteHits++
	return &pb.PrewriteResponse{}, nil
}

func (mc *mockCluster) commit(storeID uint64, regionID uint64, req *pb.CommitRequest) (*pb.CommitResponse, *pb.RegionError) {
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
		return &pb.CommitResponse{}, nil
	}
	pending := region.pending[req.GetStartVersion()]
	if pending == nil {
		return &pb.CommitResponse{}, nil
	}
	for _, key := range req.GetKeys() {
		pend, ok := pending[string(key)]
		if !ok {
			continue
		}
		region.committed[string(key)] = clusterValue{
			value:         append([]byte(nil), pend.value...),
			commitVersion: req.GetCommitVersion(),
		}
		delete(pending, string(key))
	}
	if len(pending) == 0 {
		delete(region.pending, req.GetStartVersion())
	}
	region.commitHits++
	return &pb.CommitResponse{}, nil
}

func (mc *mockCluster) get(storeID uint64, req *pb.KvGetRequest) (*pb.KvGetResponse, error) {
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
		return &pb.KvGetResponse{RegionError: notLeaderError(region)}, nil
	}
	if req.GetRequest() == nil {
		return &pb.KvGetResponse{}, nil
	}
	key := req.GetRequest().GetKey()
	version := req.GetRequest().GetVersion()
	region.getHits++
	val, ok := region.committed[string(key)]
	if !ok || val.commitVersion > version {
		return &pb.KvGetResponse{Response: &pb.GetResponse{NotFound: true}}, nil
	}
	return &pb.KvGetResponse{
		Response: &pb.GetResponse{Value: append([]byte(nil), val.value...)},
	}, nil
}

func (mc *mockCluster) scan(storeID uint64, req *pb.KvScanRequest) (*pb.KvScanResponse, error) {
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
		return &pb.KvScanResponse{RegionError: notLeaderError(region)}, nil
	}
	scanReq := req.GetRequest()
	if scanReq == nil {
		return &pb.KvScanResponse{Response: &pb.ScanResponse{}}, nil
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
	resp := &pb.ScanResponse{}
	for _, key := range keys {
		if len(resp.Kvs) >= int(limit) {
			break
		}
		if len(startKey) > 0 && key < string(startKey) {
			continue
		}
		val := region.committed[key]
		resp.Kvs = append(resp.Kvs, &pb.KV{
			Key:   []byte(key),
			Value: append([]byte(nil), val.value...),
		})
	}
	region.scanHits++
	return &pb.KvScanResponse{Response: resp}, nil
}

type mockService struct {
	storeID uint64
	cluster *mockCluster
}

func (s *mockService) KvGet(ctx context.Context, req *pb.KvGetRequest) (*pb.KvGetResponse, error) {
	return s.cluster.get(s.storeID, req)
}

func (s *mockService) KvBatchGet(ctx context.Context, req *pb.KvBatchGetRequest) (*pb.KvBatchGetResponse, error) {
	if req == nil || req.GetContext() == nil {
		return nil, statusInvalidArgument("context required")
	}
	batch := req.GetRequest()
	if batch == nil || len(batch.GetRequests()) == 0 {
		return &pb.KvBatchGetResponse{
			Response: &pb.BatchGetResponse{},
		}, nil
	}
	responses := make([]*pb.GetResponse, 0, len(batch.GetRequests()))
	for _, getReq := range batch.GetRequests() {
		if getReq == nil {
			responses = append(responses, &pb.GetResponse{NotFound: true})
			continue
		}
		resp, err := s.cluster.get(s.storeID, &pb.KvGetRequest{
			Context: req.GetContext(),
			Request: getReq,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return &pb.KvBatchGetResponse{RegionError: resp.GetRegionError()}, nil
		}
		if resp.GetResponse() != nil {
			responses = append(responses, resp.GetResponse())
		} else {
			responses = append(responses, &pb.GetResponse{NotFound: true})
		}
	}
	return &pb.KvBatchGetResponse{
		Response: &pb.BatchGetResponse{Responses: responses},
	}, nil
}

func (s *mockService) KvScan(ctx context.Context, req *pb.KvScanRequest) (*pb.KvScanResponse, error) {
	return s.cluster.scan(s.storeID, req)
}

func (s *mockService) KvPrewrite(ctx context.Context, req *pb.KvPrewriteRequest) (*pb.KvPrewriteResponse, error) {
	if req == nil || req.GetContext() == nil {
		return nil, statusInvalidArgument("context required")
	}
	resp, regionErr := s.cluster.prewrite(s.storeID, req.GetContext().GetRegionId(), req.GetRequest())
	return &pb.KvPrewriteResponse{
		Response:    resp,
		RegionError: regionErr,
	}, nil
}

func (s *mockService) KvCommit(ctx context.Context, req *pb.KvCommitRequest) (*pb.KvCommitResponse, error) {
	if req == nil || req.GetContext() == nil {
		return nil, statusInvalidArgument("context required")
	}
	resp, regionErr := s.cluster.commit(s.storeID, req.GetContext().GetRegionId(), req.GetRequest())
	return &pb.KvCommitResponse{
		Response:    resp,
		RegionError: regionErr,
	}, nil
}

func (s *mockService) KvBatchRollback(context.Context, *pb.KvBatchRollbackRequest) (*pb.KvBatchRollbackResponse, error) {
	return &pb.KvBatchRollbackResponse{}, nil
}

func (s *mockService) KvResolveLock(context.Context, *pb.KvResolveLockRequest) (*pb.KvResolveLockResponse, error) {
	return &pb.KvResolveLockResponse{}, nil
}

func (s *mockService) KvCheckTxnStatus(context.Context, *pb.KvCheckTxnStatusRequest) (*pb.KvCheckTxnStatusResponse, error) {
	return &pb.KvCheckTxnStatusResponse{}, nil
}

func startMockStore(t *testing.T, cluster *mockCluster, storeID uint64) (string, func()) {
	t.Helper()
	srv := grpc.NewServer()
	service := &mockService{storeID: storeID, cluster: cluster}
	pb.RegisterTinyKvServer(srv, service)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.Serve(lis)
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func TestClientTwoPhaseCommitAndGet(t *testing.T) {
	cluster := newMockCluster(
		clusterRegion{
			meta: &pb.RegionMeta{
				Id:               1,
				StartKey:         []byte("a"),
				EndKey:           []byte("m"),
				EpochVersion:     1,
				EpochConfVersion: 1,
				Peers: []*pb.RegionPeer{
					{StoreId: 1, PeerId: 101},
					{StoreId: 2, PeerId: 201},
				},
			},
			leaderStore: 1,
		},
		clusterRegion{
			meta: &pb.RegionMeta{
				Id:               2,
				StartKey:         []byte("m"),
				EndKey:           nil,
				EpochVersion:     1,
				EpochConfVersion: 1,
				Peers: []*pb.RegionPeer{
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

	clientCfg := client.Config{
		Stores: []client.StoreEndpoint{
			{StoreID: 2, Addr: addrFollower},
			{StoreID: 1, Addr: addrLeader},
		},
		Regions: []client.RegionConfig{
			{
				Meta:          cluster.regions[1].meta,
				LeaderStoreID: 2,
			},
			{
				Meta:          cluster.regions[2].meta,
				LeaderStoreID: 2,
			},
		},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	cli, err := client.New(clientCfg)
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mutations := []*pb.Mutation{
		{Op: pb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value-a")},
		{Op: pb.Mutation_Put, Key: []byte("zenith"), Value: []byte("value-z")},
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

// Utility helpers

func protoClone(meta *pb.RegionMeta) *pb.RegionMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*pb.RegionMeta)
}

func protoClonePeer(peer *pb.RegionPeer) *pb.RegionPeer {
	if peer == nil {
		return nil
	}
	return proto.Clone(peer).(*pb.RegionPeer)
}

func notLeaderError(region *clusterRegion) *pb.RegionError {
	return &pb.RegionError{
		NotLeader: &pb.NotLeader{
			RegionId: region.meta.GetId(),
			Leader:   leaderPeer(region.meta, region.leaderStore),
		},
	}
}

func leaderPeer(meta *pb.RegionMeta, storeID uint64) *pb.RegionPeer {
	for _, peer := range meta.GetPeers() {
		if peer.GetStoreId() == storeID {
			return protoClonePeer(peer)
		}
	}
	return nil
}

func epochNotMatch(regions map[uint64]*clusterRegion) *pb.RegionError {
	resp := &pb.RegionError{
		EpochNotMatch: &pb.EpochNotMatch{},
	}
	for _, region := range regions {
		resp.EpochNotMatch.Regions = append(resp.EpochNotMatch.Regions, protoClone(region.meta))
	}
	return resp
}

func statusInvalidArgument(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}
