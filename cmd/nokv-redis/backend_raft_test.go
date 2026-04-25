package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/config"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type stubNoKVServer struct {
	kvrpcpb.UnimplementedNoKVServer

	mu               sync.Mutex
	prewriteAttempts int
	resolveCalls     int
	checkCalls       int
	commitCalls      int
	lockVersion      uint64
	lockKey          []byte
	responses        map[string]*kvrpcpb.GetResponse
}

func (s *stubNoKVServer) KvPrewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prewriteAttempts++
	if s.lockVersion == 0 {
		s.lockVersion = 200
	}
	if s.prewriteAttempts == 1 {
		mutations := req.GetRequest().GetMutations()
		var key []byte
		if len(mutations) > 0 {
			key = mutations[0].GetKey()
		}
		primary := req.GetRequest().GetPrimaryLock()
		s.lockKey = append([]byte(nil), key...)
		lockedPrimary := &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
			PrimaryLock: append([]byte(nil), primary...),
			Key:         append([]byte(nil), key...),
			LockVersion: s.lockVersion,
			LockTtl:     1,
		}}
		return &kvrpcpb.KvPrewriteResponse{Response: &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{lockedPrimary}}}, nil
	}
	return &kvrpcpb.KvPrewriteResponse{Response: &kvrpcpb.PrewriteResponse{}}, nil
}

func (s *stubNoKVServer) KvCommit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitCalls++
	return &kvrpcpb.KvCommitResponse{Response: &kvrpcpb.CommitResponse{}}, nil
}

func (s *stubNoKVServer) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkCalls++
	return &kvrpcpb.KvCheckTxnStatusResponse{Response: &kvrpcpb.CheckTxnStatusResponse{
		Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
	}}, nil
}

func (s *stubNoKVServer) KvResolveLock(ctx context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolveCalls++
	return &kvrpcpb.KvResolveLockResponse{Response: &kvrpcpb.ResolveLockResponse{
		ResolvedLocks: uint64(len(req.GetRequest().GetKeys())),
	}}, nil
}

func (s *stubNoKVServer) KvBatchGet(ctx context.Context, req *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error) {
	responses := make([]*kvrpcpb.GetResponse, len(req.GetRequest().GetRequests()))
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range responses {
		getReq := req.GetRequest().GetRequests()[i]
		if getReq == nil {
			responses[i] = &kvrpcpb.GetResponse{NotFound: true}
			continue
		}
		if s.responses != nil {
			if resp, ok := s.responses[string(getReq.GetKey())]; ok {
				responses[i] = resp
				continue
			}
		}
		responses[i] = &kvrpcpb.GetResponse{NotFound: true}
	}
	return &kvrpcpb.KvBatchGetResponse{Response: &kvrpcpb.BatchGetResponse{Responses: responses}}, nil
}

func startStubNoKV(t *testing.T) (addr string, srv *stubNoKVServer, shutdown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tinykv stub: %v", err)
	}
	setDefaultStubStoreAddr(l.Addr().String())
	server := grpc.NewServer()
	stub := &stubNoKVServer{}
	kvrpcpb.RegisterNoKVServer(server, stub)
	go func() {
		_ = server.Serve(l)
	}()
	return l.Addr().String(), stub, func() {
		server.GracefulStop()
		_ = l.Close()
	}
}

var defaultStubStoreAddr struct {
	sync.Mutex
	addr string
}

func setDefaultStubStoreAddr(addr string) {
	defaultStubStoreAddr.Lock()
	defaultStubStoreAddr.addr = addr
	defaultStubStoreAddr.Unlock()
}

func currentDefaultStubStoreAddr() string {
	defaultStubStoreAddr.Lock()
	defer defaultStubStoreAddr.Unlock()
	return defaultStubStoreAddr.addr
}

type stubCoordinatorServer struct {
	coordpb.UnimplementedCoordinatorServer

	mu         sync.Mutex
	region     *metapb.RegionDescriptor
	storeAddr  string
	nextTS     uint64
	tsoCalls   int
	routeCalls int
	tsoErr     error
	routeErr   error
}

func (s *stubCoordinatorServer) Tso(_ context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tsoCalls++
	if s.tsoErr != nil {
		return nil, s.tsoErr
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	if s.nextTS == 0 {
		s.nextTS = 1
	}
	first := s.nextTS
	s.nextTS += count
	return &coordpb.TsoResponse{
		Timestamp:        first,
		Count:            count,
		ConsumedFrontier: first + count - 1,
	}, nil
}

func (s *stubCoordinatorServer) GetRegionByKey(_ context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routeCalls++
	if s.routeErr != nil {
		return nil, s.routeErr
	}
	resp := &coordpb.GetRegionByKeyResponse{
		ServingClass:    coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
		SyncHealth:      coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
		CatchUpState:    coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
		ServedByLeader:  true,
		ServedFreshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	}
	if req == nil {
		resp.NotFound = true
		return resp, nil
	}
	requestedFreshness := req.GetFreshness()
	if requestedFreshness == coordpb.Freshness_FRESHNESS_UNSPECIFIED {
		requestedFreshness = coordpb.Freshness_FRESHNESS_BEST_EFFORT
	}
	resp.ServedFreshness = requestedFreshness
	resp.RequiredDescriptorRevision = req.GetRequiredDescriptorRevision()
	if s.region == nil || !keyInRegion(req.GetKey(), s.region.GetStartKey(), s.region.GetEndKey()) {
		resp.NotFound = true
		return resp, nil
	}
	desc := metawire.DescriptorToProto(metawire.DescriptorFromProto(proto.Clone(s.region).(*metapb.RegionDescriptor)))
	resp.RegionDescriptor = desc
	resp.DescriptorRevision = desc.GetRootEpoch()
	return resp, nil
}

func (s *stubCoordinatorServer) GetStore(_ context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req == nil || req.GetStoreId() == 0 || s.storeAddr == "" {
		return &coordpb.GetStoreResponse{NotFound: true}, nil
	}
	return &coordpb.GetStoreResponse{Store: &coordpb.StoreInfo{
		StoreId:    req.GetStoreId(),
		ClientAddr: s.storeAddr,
		State:      coordpb.StoreState_STORE_STATE_UP,
	}}, nil
}

func (s *stubCoordinatorServer) ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.storeAddr == "" {
		return &coordpb.ListStoresResponse{}, nil
	}
	return &coordpb.ListStoresResponse{Stores: []*coordpb.StoreInfo{{
		StoreId:    1,
		ClientAddr: s.storeAddr,
		State:      coordpb.StoreState_STORE_STATE_UP,
	}}}, nil
}

func startStubCoordinator(t *testing.T, region *metapb.RegionDescriptor) (addr string, srv *stubCoordinatorServer, shutdown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	stub := &stubCoordinatorServer{
		region:    region,
		storeAddr: currentDefaultStubStoreAddr(),
		nextTS:    100,
	}
	coordpb.RegisterCoordinatorServer(server, stub)
	go func() {
		_ = server.Serve(l)
	}()
	return l.Addr().String(), stub, func() {
		server.GracefulStop()
		_ = l.Close()
	}
}

func keyInRegion(key, start, end []byte) bool {
	if len(start) > 0 && bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}

func defaultCoordinatorRegionMeta() *metapb.RegionDescriptor {
	return &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: nil,
		EndKey:   nil,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
		},
	}
}

func TestDecodeKeyVariants(t *testing.T) {
	raw := []byte("hello")
	hexVal := "hex:" + hex.EncodeToString(raw)
	base64Val := base64.StdEncoding.EncodeToString(raw)

	cases := map[string][]byte{
		"":        nil,
		"    ":    nil,
		"-":       nil,
		hexVal:    raw,
		base64Val: raw,
		"hello":   raw,
	}
	for input, expect := range cases {
		got := decodeKey(input)
		if string(expect) != string(got) {
			t.Fatalf("decodeKey(%q) = %q, expect %q", input, string(got), string(expect))
		}
	}
}

func TestCoordinatorTSOAllocatorReserveMonotonic(t *testing.T) {
	_, coord, stopCoordinator := startStubCoordinator(t, nil)
	defer stopCoordinator()

	alloc := newCoordinatorTSOAllocator(context.Background(), coord, time.Second)
	first, err := alloc.Reserve(3)
	require.NoError(t, err)
	second, err := alloc.Reserve(2)
	require.NoError(t, err)
	require.Greater(t, second, first)

	coord.mu.Lock()
	require.Equal(t, 2, coord.tsoCalls)
	coord.mu.Unlock()
}

func TestNewRaftBackendUsesDockerScopeAndTSO(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	coordAddr, coord, stopCoordinator := startStubCoordinator(t, &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: nil,
		EndKey:   nil,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
		},
	})
	defer stopCoordinator()

	cfg := config.File{
		MaxRetries: 3,
		Stores: []config.Store{
			{
				StoreID:    1,
				Addr:       "127.0.0.1:1", // intentionally invalid so docker scope must be used
				DockerAddr: storeAddr,
			},
		},
		Regions: nil,
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(cfgPath, raw, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "docker")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
		if backend.client != nil {
			_ = backend.client.Close()
		}
	}()

	if _, err := backend.reserveTimestamp(2); err != nil {
		t.Fatalf("reserve timestamp: %v", err)
	}

	val, err := backend.Get([]byte("key"))
	if err != nil {
		t.Fatalf("expected nil error from stub server, got %v", err)
	}
	if val == nil || val.Found {
		t.Fatalf("expected missing value")
	}

	coord.mu.Lock()
	require.GreaterOrEqual(t, coord.tsoCalls, 1)
	require.GreaterOrEqual(t, coord.routeCalls, 1)
	coord.mu.Unlock()
}

func TestNewRaftBackendRequiresCoordinatorAddr(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	_, err := newRaftBackend(context.Background(), cfgPath, "", "host")
	require.Error(t, err)
	require.Contains(t, err.Error(), "coordinator-addr is required")
}

func TestNewRaftBackendReadsCoordinatorAddrFromConfig(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()

	cfg := config.File{
		Coordinator: &config.Coordinator{
			Addr: coordAddr,
		},
		Stores: []config.Store{
			{
				StoreID: 1,
				Addr:    storeAddr,
			},
		},
		Regions: []config.Region{
			{
				ID: 1,
				Epoch: config.RegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []config.Peer{
					{StoreID: 1, PeerID: 101},
				},
				LeaderStoreID: 1,
			},
		},
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	backend, err := newRaftBackend(context.Background(), cfgPath, "", "host")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()

	_, err = backend.reserveTimestamp(1)
	require.NoError(t, err)
}

func TestNewRaftBackendRoutingUsesCoordinatorResolver(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	coordAddr, coord, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()

	// Keep a region in config to ensure runtime routing still goes through the Coordinator.
	cfg := config.File{
		Coordinator: &config.Coordinator{
			Addr: coordAddr,
		},
		Stores: []config.Store{
			{
				StoreID: 1,
				Addr:    storeAddr,
			},
		},
		Regions: []config.Region{
			{
				ID:       1,
				StartKey: "",
				EndKey:   "",
				Epoch: config.RegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []config.Peer{
					{StoreID: 1, PeerID: 101},
				},
				LeaderStoreID: 1,
			},
		},
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	backend, err := newRaftBackend(context.Background(), cfgPath, "", "host")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()

	_, err = backend.Get([]byte("route-via-coordinator"))
	require.NoError(t, err)

	coord.mu.Lock()
	require.GreaterOrEqual(t, coord.routeCalls, 1)
	coord.mu.Unlock()
}

func TestNewRaftBackendCLIAddrOverridesConfigCoordinator(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	validCoordAddr, _, stopValidCoord := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopValidCoord()

	cfg := config.File{
		Coordinator: &config.Coordinator{
			Addr: "127.0.0.1:0", // invalid on purpose; CLI override should win.
		},
		Stores: []config.Store{
			{
				StoreID: 1,
				Addr:    storeAddr,
			},
		},
		Regions: []config.Region{
			{
				ID: 1,
				Epoch: config.RegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []config.Peer{
					{StoreID: 1, PeerID: 101},
				},
				LeaderStoreID: 1,
			},
		},
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	backend, err := newRaftBackend(context.Background(), cfgPath, validCoordAddr, "host")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()

	_, err = backend.reserveTimestamp(1)
	require.NoError(t, err)
}

func TestRaftBackendTranslatesRouteUnavailable(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	coordAddr, coord, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()

	cfg := config.File{
		Coordinator: &config.Coordinator{Addr: coordAddr},
		Stores:      []config.Store{{StoreID: 1, Addr: storeAddr}},
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	backend, err := newRaftBackend(context.Background(), cfgPath, "", "host")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()

	coord.mu.Lock()
	coord.routeErr = status.Error(codes.Unavailable, "coordinator down")
	coord.mu.Unlock()

	_, err = backend.Get([]byte("route-unavailable"))
	require.Error(t, err)
	require.True(t, isTemporaryBackendError(err))
	require.Contains(t, err.Error(), "TRYAGAIN")
}

func TestRaftBackendTranslatesRegionNotFound(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	coordAddr, _, stopCoordinator := startStubCoordinator(t, &metapb.RegionDescriptor{
		RegionId: 1,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 101},
		},
	})
	defer stopCoordinator()

	cfg := config.File{
		Coordinator: &config.Coordinator{Addr: coordAddr},
		Stores:      []config.Store{{StoreID: 1, Addr: storeAddr}},
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	backend, err := newRaftBackend(context.Background(), cfgPath, "", "host")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()

	_, err = backend.Get([]byte("zulu"))
	require.Error(t, err)
	require.False(t, isTemporaryBackendError(err))
	require.Contains(t, err.Error(), "ERR region route not found during read")
}

func TestRaftBackendResolveLockConflict(t *testing.T) {
	storeAddr, stub, stopStore := startStubNoKV(t)
	defer stopStore()

	cfg := config.File{
		MaxRetries: 3,
		Stores: []config.Store{
			{
				StoreID: 1,
				Addr:    storeAddr,
			},
		},
		Regions: []config.Region{
			{
				ID: 1,
				Epoch: config.RegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []config.Peer{
					{StoreID: 1, PeerID: 101},
				},
				LeaderStoreID: 1,
			},
		},
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(cfgPath, raw, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()

	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
		if backend.client != nil {
			_ = backend.client.Close()
		}
	}()

	if _, err := backend.Set(setArgs{Key: []byte("conflict-key"), Value: []byte("value")}); err != nil {
		t.Fatalf("set with conflict resolution: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.prewriteAttempts < 2 {
		t.Fatalf("expected prewrite retries due to conflict, got %d attempts", stub.prewriteAttempts)
	}
	if stub.checkCalls == 0 {
		t.Fatalf("expected CheckTxnStatus to be invoked")
	}
	if stub.resolveCalls == 0 {
		t.Fatalf("expected ResolveLock to be invoked")
	}
	if stub.commitCalls == 0 {
		t.Fatalf("expected commit to run after resolving lock")
	}
}

func TestRaftBackendGetWithTTL(t *testing.T) {
	storeAddr, stub, stopStore := startStubNoKV(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()
	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key := []byte("ttl-key")
	valueKey := string(key)
	expireAt := uint64(time.Now().Add(time.Hour).Unix())

	stub.mu.Lock()
	stub.responses = map[string]*kvrpcpb.GetResponse{
		valueKey: {Value: []byte("value"), ExpiresAt: expireAt},
	}
	stub.mu.Unlock()

	val, err := backend.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !val.Found {
		t.Fatalf("expected value to be found")
	}
	if val.ExpiresAt != expireAt {
		t.Fatalf("expected expire %d, got %d", expireAt, val.ExpiresAt)
	}
}

func TestRaftBackendExpireCleanup(t *testing.T) {
	storeAddr, stub, stopStore := startStubNoKV(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()
	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key := []byte("expired-key")
	expireAt := uint64(time.Now().Add(-time.Hour).Unix())

	stub.mu.Lock()
	stub.responses = map[string]*kvrpcpb.GetResponse{
		string(key): {Value: []byte("value"), ExpiresAt: expireAt},
	}
	before := stub.commitCalls
	stub.mu.Unlock()

	val, err := backend.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val.Found {
		t.Fatalf("expected expired value to be deleted")
	}

	stub.mu.Lock()
	after := stub.commitCalls
	stub.mu.Unlock()
	if after <= before {
		t.Fatalf("expected delete mutation to commit")
	}
}

func TestRaftBackendIncrByAndErrors(t *testing.T) {
	storeAddr, stub, stopStore := startStubNoKV(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()
	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key := []byte("counter")
	stub.mu.Lock()
	stub.responses = map[string]*kvrpcpb.GetResponse{
		string(key): {Value: []byte("10")},
	}
	stub.mu.Unlock()

	val, err := backend.IncrBy(key, 5)
	if err != nil {
		t.Fatalf("incrby: %v", err)
	}
	if val != 15 {
		t.Fatalf("expected 15, got %d", val)
	}

	stub.mu.Lock()
	stub.responses[string(key)] = &kvrpcpb.GetResponse{Value: []byte("abc")}
	stub.mu.Unlock()
	if _, err := backend.IncrBy(key, 1); err == nil {
		t.Fatalf("expected non-integer error")
	}

	stub.mu.Lock()
	stub.responses[string(key)] = &kvrpcpb.GetResponse{Value: []byte(strconv.FormatInt(math.MaxInt64, 10))}
	stub.mu.Unlock()
	if _, err := backend.IncrBy(key, 1); err == nil {
		t.Fatalf("expected overflow error")
	}
}

func TestRaftBackendMGetAndExists(t *testing.T) {
	storeAddr, stub, stopStore := startStubNoKV(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()
	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key1 := []byte("k1")
	key2 := []byte("k2")
	key3 := []byte("k3")
	futureExpireAt := uint64(time.Now().Add(time.Hour).Unix())
	pastExpireAt := uint64(time.Now().Add(-time.Hour).Unix())

	stub.mu.Lock()
	stub.responses = map[string]*kvrpcpb.GetResponse{
		string(key1): {Value: []byte("v1")},
		string(key2): {Value: []byte("v2"), ExpiresAt: futureExpireAt},
		string(key3): {Value: []byte("v3"), ExpiresAt: pastExpireAt},
	}
	stub.mu.Unlock()

	vals, err := backend.MGet([][]byte{key1, key2, key3})
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	if !vals[0].Found || string(vals[0].Value) != "v1" {
		t.Fatalf("expected k1 to be found")
	}
	if !vals[1].Found || vals[1].ExpiresAt != futureExpireAt {
		t.Fatalf("expected k2 ttl %d, got %+v", futureExpireAt, vals[1])
	}
	if vals[2].Found {
		t.Fatalf("expected k3 to be missing")
	}

	count, err := backend.Exists([][]byte{key1, key2, key3})
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 existing keys, got %d", count)
	}

	stub.mu.Lock()
	commits := stub.commitCalls
	stub.mu.Unlock()
	if commits == 0 {
		t.Fatalf("expected cleanup delete to commit")
	}
}

func TestRaftBackendMSetAndDel(t *testing.T) {
	storeAddr, stub, stopStore := startStubNoKV(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()
	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	if err := backend.MSet([][2][]byte{{[]byte("a"), []byte("1")}, {[]byte("b"), []byte("2")}}); err != nil {
		t.Fatalf("mset: %v", err)
	}
	if err := backend.MSet([][2][]byte{{nil, []byte("bad")}}); err == nil {
		t.Fatalf("expected error for empty key")
	}

	stub.mu.Lock()
	before := stub.commitCalls
	stub.responses = map[string]*kvrpcpb.GetResponse{
		"a": {Value: []byte("1")},
	}
	stub.mu.Unlock()

	removed, err := backend.Del([][]byte{[]byte("a"), []byte("missing")})
	if err != nil {
		t.Fatalf("del: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	stub.mu.Lock()
	after := stub.commitCalls
	stub.mu.Unlock()
	if after <= before {
		t.Fatalf("expected delete commit")
	}
}

func writeBackendConfig(t *testing.T, storeAddr string) string {
	t.Helper()
	cfg := config.File{
		Stores: []config.Store{
			{
				StoreID: 1,
				Addr:    storeAddr,
			},
		},
		Regions: []config.Region{
			{
				ID:       1,
				StartKey: "",
				EndKey:   "",
				Epoch: config.RegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []config.Peer{
					{StoreID: 1, PeerID: 101},
				},
				LeaderStoreID: 1,
			},
		},
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "raft_config.json")
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(cfgPath, raw, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return cfgPath
}

type stubTSO struct {
	next uint64
	err  error
}

func (s *stubTSO) Reserve(n uint64) (uint64, error) {
	if s.err != nil {
		return 0, s.err
	}
	if n == 0 {
		return 0, fmt.Errorf("reserve: n must be >= 1")
	}
	start := s.next + 1
	s.next += n
	return start, nil
}

type stubRaftClient struct {
	batchGet    map[string]*kvrpcpb.GetResponse
	batchGetErr error
	batchGetFn  func(keys [][]byte) (map[string]*kvrpcpb.GetResponse, error) // Custom BatchGet logic
	mutateErr   error
	mutateFn    func() error
	mutateCalls int
	lastMuts    []*kvrpcpb.Mutation
	checkResp   *kvrpcpb.CheckTxnStatusResponse
	checkErr    error
	checkCalls  int // Track CheckTxnStatus call count
	resolveResp uint64
	resolveErr  error
	resolveKeys [][]byte
}

func (s *stubRaftClient) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error) {
	if s.batchGetFn != nil {
		return s.batchGetFn(keys)
	}
	if s.batchGetErr != nil {
		return nil, s.batchGetErr
	}
	out := make(map[string]*kvrpcpb.GetResponse, len(keys))
	for _, key := range keys {
		k := string(key)
		if resp, ok := s.batchGet[k]; ok {
			out[k] = resp
			continue
		}
		out[k] = &kvrpcpb.GetResponse{NotFound: true}
	}
	return out, nil
}

func (s *stubRaftClient) Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	s.mutateCalls++
	s.lastMuts = s.lastMuts[:0]
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		s.lastMuts = append(s.lastMuts, proto.Clone(mut).(*kvrpcpb.Mutation))
	}
	if s.mutateFn != nil {
		return s.mutateFn()
	}
	if s.mutateErr != nil {
		return s.mutateErr
	}
	return nil
}

func (s *stubRaftClient) CheckTxnStatus(ctx context.Context, primary []byte, lockVersion, currentTS uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
	s.checkCalls++
	return s.checkResp, s.checkErr
}

func (s *stubRaftClient) ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	s.resolveKeys = s.resolveKeys[:0]
	for _, key := range keys {
		s.resolveKeys = append(s.resolveKeys, append([]byte(nil), key...))
	}
	if s.resolveErr != nil {
		return 0, s.resolveErr
	}
	if s.resolveResp != 0 {
		return s.resolveResp, nil
	}
	return uint64(len(keys)), nil
}

func (s *stubRaftClient) Close() error {
	return nil
}

func (s *stubRaftClient) keyResolved(key []byte) bool {
	for _, resolvedKey := range s.resolveKeys {
		if bytes.Equal(key, resolvedKey) {
			return true
		}
	}
	return false
}

func newStubBackend() (*raftBackend, *stubRaftClient, *stubTSO) {
	client := &stubRaftClient{}
	ts := &stubTSO{next: 100}
	return &raftBackend{client: client, ts: ts}, client, ts
}

func TestCoordinatorTSOAllocatorErrors(t *testing.T) {
	alloc := newCoordinatorTSOAllocator(context.Background(), nil, time.Second)
	_, err := alloc.Reserve(1)
	require.Error(t, err)

	_, coord, stopCoordinator := startStubCoordinator(t, nil)
	defer stopCoordinator()
	coord.tsoErr = errors.New("boom")
	alloc = newCoordinatorTSOAllocator(context.Background(), coord, time.Second)
	_, err = alloc.Reserve(1)
	require.Error(t, err)

	coord.tsoErr = nil
	_, err = alloc.Reserve(0)
	require.Error(t, err)
}

func TestNewRaftBackendErrors(t *testing.T) {
	_, err := newRaftBackend(context.Background(), filepath.Join(t.TempDir(), "missing.json"), "127.0.0.1:1", "host")
	require.Error(t, err)

	cfg := config.File{
		Stores: []config.Store{
			{StoreID: 1, Addr: "127.0.0.1:1"},
			{StoreID: 1, Addr: "127.0.0.1:2"},
		},
		Regions: []config.Region{
			{
				ID: 1,
				Epoch: config.RegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []config.Peer{
					{StoreID: 1, PeerID: 101},
				},
			},
		},
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.json")
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))
	_, err = newRaftBackend(context.Background(), cfgPath, "127.0.0.1:1", "host")
	require.Error(t, err)

	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	cfg = config.File{
		Stores: []config.Store{{StoreID: 1, Addr: storeAddr}},
		Regions: []config.Region{{
			ID: 1,
			Epoch: config.RegionEpoch{
				Version:     1,
				ConfVersion: 1,
			},
			Peers: []config.Peer{{StoreID: 1, PeerID: 101}},
		}},
	}
	raw, err = json.Marshal(cfg)
	require.NoError(t, err)
	cfgPath = filepath.Join(dir, "cfg2.json")
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	_, err = newRaftBackend(context.Background(), cfgPath, "", "host")
	require.Error(t, err)
	require.Contains(t, err.Error(), "coordinator-addr is required")

	coordAddr, _, stopCoordinator := startStubCoordinator(t, defaultCoordinatorRegionMeta())
	defer stopCoordinator()

	backend, err := newRaftBackend(context.Background(), cfgPath, coordAddr, "host")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()
	_, ok := backend.ts.(*coordinatorTSOAllocator)
	require.True(t, ok)

	_, err = backend.reserveTimestamp(2)
	require.NoError(t, err)
}

func TestNewRaftBackendInvalidCoordAddress(t *testing.T) {
	storeAddr, _, stopStore := startStubNoKV(t)
	defer stopStore()
	cfgPath := writeBackendConfig(t, storeAddr)
	_, err := newRaftBackend(context.Background(), cfgPath, "127.0.0.1:0", "host")
	require.Error(t, err)
}

func TestRaftBackendCloseNil(t *testing.T) {
	var backend *raftBackend
	require.NoError(t, backend.Close())
	backend = &raftBackend{}
	require.NoError(t, backend.Close())
}

func TestRaftBackendGetErrors(t *testing.T) {
	backend, stub, ts := newStubBackend()
	ts.err = errors.New("boom")
	_, err := backend.Get([]byte("k"))
	require.Error(t, err)

	ts.err = nil
	stub.batchGetErr = errors.New("batch")
	_, err = backend.Get([]byte("k"))
	require.Error(t, err)
}

func TestRaftBackendSetBranches(t *testing.T) {
	backend, stub, _ := newStubBackend()
	_, err := backend.Set(setArgs{Key: nil})
	require.Error(t, err)

	stub.batchGet = map[string]*kvrpcpb.GetResponse{
		"key": {Value: []byte("v")},
	}
	ok, err := backend.Set(setArgs{Key: []byte("key"), Value: []byte("v2"), NX: true})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, stub.mutateCalls)

	stub.batchGet = map[string]*kvrpcpb.GetResponse{}
	ok, err = backend.Set(setArgs{Key: []byte("missing"), Value: []byte("v"), XX: true})
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = backend.Set(setArgs{Key: []byte("ttl"), Value: []byte("v"), ExpireAt: 123})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, stub.lastMuts, 1)
	require.Equal(t, kvrpcpb.Mutation_Put, stub.lastMuts[0].GetOp())
	require.Equal(t, uint64(123), stub.lastMuts[0].GetExpiresAt())

	stub.mutateErr = errors.New("mutate")
	_, err = backend.Set(setArgs{Key: []byte("err"), Value: []byte("v")})
	require.Error(t, err)
}

func TestRaftBackendDelBranches(t *testing.T) {
	backend, stub, ts := newStubBackend()
	count, err := backend.Del(nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	ts.err = errors.New("boom")
	_, err = backend.Del([][]byte{[]byte("k")})
	require.Error(t, err)

	ts.err = nil
	stub.batchGetErr = errors.New("batch")
	_, err = backend.Del([][]byte{[]byte("k")})
	require.Error(t, err)

	stub.batchGetErr = nil
	stub.batchGet = map[string]*kvrpcpb.GetResponse{
		"k": {Value: []byte("v")},
	}
	stub.mutateErr = errors.New("mutate")
	_, err = backend.Del([][]byte{[]byte("k")})
	require.Error(t, err)
}

func TestRaftBackendMGetErrors(t *testing.T) {
	backend, stub, ts := newStubBackend()
	vals, err := backend.MGet(nil)
	require.NoError(t, err)
	require.Nil(t, vals)

	ts.err = errors.New("boom")
	_, err = backend.MGet([][]byte{[]byte("k")})
	require.Error(t, err)

	ts.err = nil
	stub.batchGetErr = errors.New("batch")
	_, err = backend.MGet([][]byte{[]byte("k")})
	require.Error(t, err)

	stub.batchGetErr = nil
	stub.batchGet = map[string]*kvrpcpb.GetResponse{
		"k": {Value: []byte("v"), ExpiresAt: uint64(time.Now().Add(-time.Hour).Unix())},
	}
	stub.mutateErr = errors.New("mutate")
	_, err = backend.MGet([][]byte{[]byte("k")})
	require.Error(t, err)
}

func TestRaftBackendMSetBranches(t *testing.T) {
	backend, stub, _ := newStubBackend()
	require.NoError(t, backend.MSet(nil))
	require.Error(t, backend.MSet([][2][]byte{{nil, []byte("v")}}))
	require.NoError(t, backend.MSet([][2][]byte{{[]byte("k"), []byte("v")}, {[]byte("k2"), []byte("v2")}}))
	require.Len(t, stub.lastMuts, 2)
	require.Equal(t, uint64(0), stub.lastMuts[0].GetExpiresAt())
	require.Equal(t, uint64(0), stub.lastMuts[1].GetExpiresAt())

	stub.mutateErr = errors.New("mutate")
	require.Error(t, backend.MSet([][2][]byte{{[]byte("k"), []byte("v")}}))
}

func TestRaftBackendExists(t *testing.T) {
	backend, stub, ts := newStubBackend()
	stub.batchGet = map[string]*kvrpcpb.GetResponse{
		"a": {Value: []byte("1")},
	}
	count, err := backend.Exists([][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	ts.err = errors.New("boom")
	_, err = backend.Exists([][]byte{[]byte("a")})
	require.Error(t, err)
}

func TestRaftBackendIncrByErrors(t *testing.T) {
	backend, stub, ts := newStubBackend()
	ts.err = errors.New("boom")
	_, err := backend.IncrBy([]byte("k"), 1)
	require.Error(t, err)

	ts.err = nil
	stub.batchGetErr = errors.New("batch")
	_, err = backend.IncrBy([]byte("k"), 1)
	require.Error(t, err)

	stub.batchGetErr = nil
	stub.batchGet = map[string]*kvrpcpb.GetResponse{
		"k": {Value: []byte("abc")},
	}
	_, err = backend.IncrBy([]byte("k"), 1)
	require.ErrorIs(t, err, errNotInteger)

	stub.batchGet = map[string]*kvrpcpb.GetResponse{
		"k": {Value: fmt.Appendf(nil, "%d", math.MaxInt64)},
	}
	_, err = backend.IncrBy([]byte("k"), 1)
	require.ErrorIs(t, err, errOverflow)

	stub.batchGet = map[string]*kvrpcpb.GetResponse{
		"k": {Value: fmt.Appendf(nil, "%d", math.MinInt64)},
	}
	_, err = backend.IncrBy([]byte("k"), -1)
	require.ErrorIs(t, err, errOverflow)
}

func TestBuildValueAtVersionBranches(t *testing.T) {
	backend, stub, _ := newStubBackend()
	key := []byte("k1")
	futureExpire := uint64(time.Now().Unix() + 100)

	val, err := backend.buildValueAtVersion(key, &kvrpcpb.GetResponse{NotFound: true})
	require.NoError(t, err)
	require.False(t, val.Found)
	require.Zero(t, stub.mutateCalls)

	stub.mutateCalls = 0
	val, err = backend.buildValueAtVersion(key, &kvrpcpb.GetResponse{
		Value:     []byte("v"),
		ExpiresAt: uint64(time.Now().Add(-time.Hour).Unix()),
	})
	require.NoError(t, err)
	require.False(t, val.Found)
	require.NotZero(t, stub.mutateCalls)

	stub.mutateCalls = 0
	val, err = backend.buildValueAtVersion(key, &kvrpcpb.GetResponse{Value: []byte("v"), ExpiresAt: futureExpire})
	require.NoError(t, err)
	require.True(t, val.Found)
	require.Equal(t, []byte("v"), val.Value)
	require.Equal(t, futureExpire, val.ExpiresAt)
}

func TestMutatePaths(t *testing.T) {
	backend, stub, ts := newStubBackend()
	require.NoError(t, backend.mutate([]byte("k")))

	ts.err = errors.New("reserve")
	err := backend.mutate([]byte("k"), &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)

	ts.err = nil
	stub.mutateErr = errors.New("boom")
	err = backend.mutate([]byte("k"), &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)

	conflict := &client.KeyConflictError{Errors: []*kvrpcpb.KeyError{{Locked: &kvrpcpb.Locked{
		Key:         []byte("k"),
		PrimaryLock: []byte("k"),
		LockVersion: 1,
	}}}}
	stub.mutateErr = conflict
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusNoAction}
	err = backend.mutate([]byte("k"), &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)

	stub.mutateErr = nil
	stub.mutateFn = func() error { return conflict }
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback}
	stub.resolveErr = nil
	err = backend.mutate([]byte("k"), &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "retries exhausted")
}

func TestResolveKeyConflictsAndLocks(t *testing.T) {
	backend, stub, _ := newStubBackend()
	require.Error(t, backend.resolveKeyConflicts(nil))
	require.Error(t, backend.resolveKeyConflicts(&client.KeyConflictError{}))

	conflicts := &client.KeyConflictError{Errors: []*kvrpcpb.KeyError{nil, {}}}
	require.NoError(t, backend.resolveKeyConflicts(conflicts))

	lock := &kvrpcpb.Locked{
		PrimaryLock: []byte("p"),
		Key:         []byte("k"),
		LockVersion: 1,
	}
	stub.checkErr = errors.New("check")
	require.Error(t, backend.resolveSingleLock(lock))

	stub.checkErr = nil
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{CommitVersion: 1}
	// The primary is committed, the lock should be resolved successfully
	require.NoError(t, backend.resolveSingleLock(lock))

	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusNoAction}
	require.Error(t, backend.resolveSingleLock(lock))

	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback}
	stub.resolveErr = errors.New("resolve")
	require.Error(t, backend.resolveSingleLock(lock))

	stub.resolveErr = nil
	require.NoError(t, backend.resolveSingleLock(lock))
	require.NotEmpty(t, stub.resolveKeys)
}

func TestResolveSingleLockTranslatesRouteUnavailable(t *testing.T) {
	backend, stub, _ := newStubBackend()
	lock := &kvrpcpb.Locked{
		PrimaryLock: []byte("p"),
		Key:         []byte("k"),
		LockVersion: 1,
	}

	stub.checkErr = &client.RouteUnavailableError{
		Key: []byte("k"),
		Err: errors.New("coordinator unavailable"),
	}
	err := backend.resolveSingleLock(lock)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TRYAGAIN")
}

// TestConflictingTransactionWithCommittedPrimary simulates a complete scenario:
// Two conflicting transactions where one has its primary committed but secondary not.
func TestConflictingTransactionWithCommittedPrimary(t *testing.T) {
	backend, stub, _ := newStubBackend()

	primaryKey := []byte("key1")
	secondaryKey := []byte("key2")
	lockVersion := uint64(100)
	commitVersion := uint64(101)

	mutateCallCount := 0

	// Simulate the conflict scenario:
	// First call: returns KeyConflictError with locked secondary
	// Second call: should succeed if lock is resolved
	stub.mutateFn = func() error {
		mutateCallCount++
		if stub.keyResolved(secondaryKey) {
			// Simulate the secondary lock being resolved and committed with the same commit version as primary
			return nil
		}
		return &client.KeyConflictError{
			Errors: []*kvrpcpb.KeyError{
				{
					Locked: &kvrpcpb.Locked{
						PrimaryLock: primaryKey,
						Key:         secondaryKey,
						LockVersion: lockVersion,
						LockTtl:     3000,
					},
				},
			},
		}
	}

	// CheckTxnStatus returns: primary is already committed
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{
		CommitVersion: commitVersion,
	}

	// Execute mutate - this will encounter the lock and try to resolve
	err := backend.mutate(primaryKey, &kvrpcpb.Mutation{
		Op:    kvrpcpb.Mutation_Put,
		Key:   primaryKey,
		Value: []byte("value"),
	})

	// Verify that ResolveLocks was called with the expected secondary key
	require.NotEmpty(t, stub.resolveKeys, "ResolveLocks should be called to resolve the secondary lock")
	require.True(t, stub.keyResolved(secondaryKey), "ResolveLocks should be called with the secondary key")

	require.Equal(t, 2, mutateCallCount, "mutate should be called twice due to conflict resolution")
	require.NoError(t, err, "mutate should succeed after resolving conflict")
}

func TestReadResolveSecondaryLockAfterPrimaryCommit(t *testing.T) {
	backend, stub, _ := newStubBackend()

	key := []byte("secondary-key")
	primaryKey := []byte("primary-key")
	lockVersion := uint64(100)
	commitVersion := uint64(101)

	callCount := 0

	// Setup batchGetFn: first call returns lock conflict, after resolution returns value
	stub.batchGetFn = func(keys [][]byte) (map[string]*kvrpcpb.GetResponse, error) {
		callCount++
		for _, k := range keys {
			if bytes.Equal(k, key) && !stub.keyResolved(k) {
				return nil, &client.KeyConflictError{
					Errors: []*kvrpcpb.KeyError{{
						Locked: &kvrpcpb.Locked{
							PrimaryLock: primaryKey,
							Key:         k,
							LockVersion: lockVersion,
							LockTtl:     3000,
						},
					}},
				}
			}
		}
		out := make(map[string]*kvrpcpb.GetResponse)
		for _, k := range keys {
			if bytes.Equal(k, key) {
				out[string(k)] = &kvrpcpb.GetResponse{Value: []byte("committed-value")}
			} else {
				out[string(k)] = &kvrpcpb.GetResponse{NotFound: true}
			}
		}
		return out, nil
	}

	// CheckTxnStatus returns: primary is already committed
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{CommitVersion: commitVersion}

	val, err := backend.Get(key)
	require.NoError(t, err)
	require.True(t, val.Found, "value should be found after resolving lock")
	require.Equal(t, []byte("committed-value"), val.Value)
	require.True(t, stub.checkCalls > 0, "CheckTxnStatus should be called")
	require.True(t, len(stub.resolveKeys) > 0, "ResolveLocks should be called")
}

func TestReadDoesNotResolveIfPrimaryAlive(t *testing.T) {
	backend, stub, _ := newStubBackend()

	key := []byte("locked-key")
	primaryKey := []byte("primary-key")
	lockVersion := uint64(100)

	// Always return lock conflict
	stub.batchGetFn = func(keys [][]byte) (map[string]*kvrpcpb.GetResponse, error) {
		return nil, &client.KeyConflictError{
			Errors: []*kvrpcpb.KeyError{{
				Locked: &kvrpcpb.Locked{
					PrimaryLock: primaryKey,
					Key:         key,
					LockVersion: lockVersion,
					LockTtl:     3000,
				},
			}},
		}
	}

	// CheckTxnStatus returns: primary is still alive (no action needed)
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{
		Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusNoAction,
	}

	_, err := backend.Get(key)
	require.Error(t, err, "should return error when primary is still alive")
	require.True(t, stub.checkCalls > 0, "CheckTxnStatus should be called")
	require.Empty(t, stub.resolveKeys, "ResolveLocks should NOT be called when primary is alive")
}

func TestReadResolveRollback(t *testing.T) {
	backend, stub, _ := newStubBackend()

	secondaryKey := []byte("rollback-key")
	primaryKey := []byte("primary-key")
	lockVersion := uint64(100)

	stub.batchGetFn = func(keys [][]byte) (map[string]*kvrpcpb.GetResponse, error) {
		for _, k := range keys {
			if bytes.Equal(k, secondaryKey) && !stub.keyResolved(k) {
				return nil, &client.KeyConflictError{
					Errors: []*kvrpcpb.KeyError{{
						Locked: &kvrpcpb.Locked{
							PrimaryLock: primaryKey,
							Key:         k,
							LockVersion: lockVersion,
							LockTtl:     3000,
						},
					}},
				}
			}
		}
		out := make(map[string]*kvrpcpb.GetResponse)
		for _, k := range keys {
			out[string(k)] = &kvrpcpb.GetResponse{NotFound: true}
		}
		return out, nil
	}

	// CheckTxnStatus returns: primary lock does not exist (rolled back)
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{
		Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback,
	}

	val, err := backend.Get(secondaryKey)
	require.NoError(t, err)
	require.False(t, val.Found, "value should not be found after rollback")
	require.True(t, stub.checkCalls > 0, "CheckTxnStatus should be called")
	require.True(t, len(stub.resolveKeys) > 0, "ResolveLocks should be called for rollback")
}

func TestReadResolveLockWhenTTLExpired(t *testing.T) {
	backend, stub, _ := newStubBackend()

	secondaryKey := []byte("ttl-expired-key")
	primaryKey := []byte("primary-key")
	lockVersion := uint64(100)

	stub.batchGetFn = func(keys [][]byte) (map[string]*kvrpcpb.GetResponse, error) {
		for _, k := range keys {
			if bytes.Equal(k, secondaryKey) && !stub.keyResolved(k) {
				return nil, &client.KeyConflictError{
					Errors: []*kvrpcpb.KeyError{{
						Locked: &kvrpcpb.Locked{
							PrimaryLock: primaryKey,
							Key:         k,
							LockVersion: lockVersion,
							LockTtl:     1, // Very short TTL
						},
					}},
				}
			}
		}
		out := make(map[string]*kvrpcpb.GetResponse)
		for _, k := range keys {
			out[string(k)] = &kvrpcpb.GetResponse{NotFound: true}
		}
		return out, nil
	}

	// CheckTxnStatus returns: TTL expired, should rollback
	stub.checkResp = &kvrpcpb.CheckTxnStatusResponse{
		Action: kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
	}

	val, err := backend.Get(secondaryKey)
	require.NoError(t, err)
	require.False(t, val.Found, "value should not be found after TTL-expired rollback")
	require.True(t, stub.checkCalls > 0, "CheckTxnStatus should be called")
	require.True(t, len(stub.resolveKeys) > 0, "ResolveLocks should be called when TTL expired")
}
