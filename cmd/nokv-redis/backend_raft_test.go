package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type stubTinyKvServer struct {
	pb.UnimplementedTinyKvServer

	mu               sync.Mutex
	prewriteAttempts int
	resolveCalls     int
	checkCalls       int
	commitCalls      int
	lockVersion      uint64
	lockKey          []byte
	responses        map[string]*pb.GetResponse
}

func (s *stubTinyKvServer) KvPrewrite(ctx context.Context, req *pb.KvPrewriteRequest) (*pb.KvPrewriteResponse, error) {
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
		lockedPrimary := &pb.KeyError{Locked: &pb.Locked{
			PrimaryLock: append([]byte(nil), primary...),
			Key:         append([]byte(nil), key...),
			LockVersion: s.lockVersion,
			LockTtl:     1,
		}}
		metaKey := ttlMetaKey(key)
		lockedMeta := &pb.KeyError{Locked: &pb.Locked{
			PrimaryLock: append([]byte(nil), primary...),
			Key:         metaKey,
			LockVersion: s.lockVersion,
			LockTtl:     1,
			LockType:    pb.Mutation_Delete,
		}}
		return &pb.KvPrewriteResponse{Response: &pb.PrewriteResponse{Errors: []*pb.KeyError{lockedPrimary, lockedMeta}}}, nil
	}
	return &pb.KvPrewriteResponse{Response: &pb.PrewriteResponse{}}, nil
}

func (s *stubTinyKvServer) KvCommit(ctx context.Context, req *pb.KvCommitRequest) (*pb.KvCommitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitCalls++
	return &pb.KvCommitResponse{Response: &pb.CommitResponse{}}, nil
}

func (s *stubTinyKvServer) KvCheckTxnStatus(ctx context.Context, req *pb.KvCheckTxnStatusRequest) (*pb.KvCheckTxnStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkCalls++
	return &pb.KvCheckTxnStatusResponse{Response: &pb.CheckTxnStatusResponse{
		Action: pb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
	}}, nil
}

func (s *stubTinyKvServer) KvResolveLock(ctx context.Context, req *pb.KvResolveLockRequest) (*pb.KvResolveLockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolveCalls++
	return &pb.KvResolveLockResponse{Response: &pb.ResolveLockResponse{
		ResolvedLocks: uint64(len(req.GetRequest().GetKeys())),
	}}, nil
}

func (s *stubTinyKvServer) KvBatchGet(ctx context.Context, req *pb.KvBatchGetRequest) (*pb.KvBatchGetResponse, error) {
	responses := make([]*pb.GetResponse, len(req.GetRequest().GetRequests()))
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range responses {
		getReq := req.GetRequest().GetRequests()[i]
		if getReq == nil {
			responses[i] = &pb.GetResponse{NotFound: true}
			continue
		}
		if s.responses != nil {
			if resp, ok := s.responses[string(getReq.GetKey())]; ok {
				responses[i] = resp
				continue
			}
		}
		responses[i] = &pb.GetResponse{NotFound: true}
	}
	return &pb.KvBatchGetResponse{Response: &pb.BatchGetResponse{Responses: responses}}, nil
}

func startStubTinyKv(t *testing.T) (addr string, srv *stubTinyKvServer, shutdown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tinykv stub: %v", err)
	}
	server := grpc.NewServer()
	stub := &stubTinyKvServer{}
	pb.RegisterTinyKvServer(server, stub)
	go func() {
		_ = server.Serve(l)
	}()
	return l.Addr().String(), stub, func() {
		server.GracefulStop()
		_ = l.Close()
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

func TestLocalOracleReserveMonotonic(t *testing.T) {
	var oracle localOracle
	first, err := oracle.Reserve(3)
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	second, err := oracle.Reserve(2)
	if err != nil {
		t.Fatalf("reserve second: %v", err)
	}
	if second <= first {
		t.Fatalf("monotonicity violated: first=%d second=%d", first, second)
	}
}

func TestNewRaftBackendUsesDockerScopeAndTSO(t *testing.T) {
	storeAddr, _, stopStore := startStubTinyKv(t)
	defer stopStore()

	tsoCalls := make(chan uint64, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		batch := r.URL.Query().Get("batch")
		if batch == "" {
			http.Error(w, "missing batch", http.StatusBadRequest)
			return
		}
		const baseTs = 100
		payload := struct {
			Timestamp uint64 `json:"timestamp"`
			Count     uint64 `json:"count"`
		}{
			Timestamp: baseTs,
			Count:     2,
		}
		_ = json.NewEncoder(w).Encode(&payload)
		select {
		case tsoCalls <- payload.Timestamp:
		default:
		}
	}))
	defer ts.Close()

	cfg := config.File{
		MaxRetries: 3,
		Stores: []config.Store{
			{
				StoreID:    1,
				Addr:       "127.0.0.1:1", // intentionally invalid so docker scope must be used
				DockerAddr: storeAddr,
			},
		},
		Regions: []config.Region{
			{
				ID:       1,
				StartKey: "a",
				EndKey:   "-",
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
		TSO: &config.TSO{
			AdvertiseURL: ts.URL,
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

	backend, err := newRaftBackend(cfgPath, "", "docker")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
		if backend.client != nil {
			_ = backend.client.Close()
		}
	}()

	httpTSO, ok := backend.ts.(*httpTSO)
	if !ok {
		t.Fatalf("expected httpTSO allocator, got %T", backend.ts)
	}
	if httpTSO.url != ts.URL {
		t.Fatalf("unexpected TSO url: %s", httpTSO.url)
	}

	if _, err := backend.reserveTimestamp(2); err != nil {
		t.Fatalf("reserve timestamp: %v", err)
	}

	select {
	case <-tsoCalls:
	case <-time.After(2 * time.Second):
		t.Fatalf("http TSO not invoked")
	}
}

func TestNewRaftBackendFallsBackToLocalOracle(t *testing.T) {
	storeAddr, _, stopStore := startStubTinyKv(t)
	defer stopStore()

	cfg := config.File{
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

	backend, err := newRaftBackend(cfgPath, "", "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
		if backend.client != nil {
			_ = backend.client.Close()
		}
	}()

	if _, ok := backend.ts.(*localOracle); !ok {
		t.Fatalf("expected localOracle allocator, got %T", backend.ts)
	}

	val, err := backend.Get([]byte("key"))
	if err != nil {
		t.Fatalf("expected nil error from stub server, got %v", err)
	}
	if val == nil || val.Found {
		t.Fatalf("expected missing value")
	}
}

func TestRaftBackendResolveLockConflict(t *testing.T) {
	storeAddr, stub, stopStore := startStubTinyKv(t)
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

	backend, err := newRaftBackend(cfgPath, "", "host")
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
	storeAddr, stub, stopStore := startStubTinyKv(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	backend, err := newRaftBackend(cfgPath, "", "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key := []byte("ttl-key")
	valueKey := string(key)
	expireAt := uint64(time.Now().Add(time.Hour).Unix())
	ttlBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBuf, expireAt)

	stub.mu.Lock()
	stub.responses = map[string]*pb.GetResponse{
		valueKey:                {Value: []byte("value")},
		string(ttlMetaKey(key)): {Value: ttlBuf},
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
	storeAddr, stub, stopStore := startStubTinyKv(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	backend, err := newRaftBackend(cfgPath, "", "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key := []byte("expired-key")
	expireAt := uint64(time.Now().Add(-time.Hour).Unix())
	ttlBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBuf, expireAt)

	stub.mu.Lock()
	stub.responses = map[string]*pb.GetResponse{
		string(key):             {Value: []byte("value")},
		string(ttlMetaKey(key)): {Value: ttlBuf},
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
	storeAddr, stub, stopStore := startStubTinyKv(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	backend, err := newRaftBackend(cfgPath, "", "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key := []byte("counter")
	stub.mu.Lock()
	stub.responses = map[string]*pb.GetResponse{
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
	stub.responses[string(key)] = &pb.GetResponse{Value: []byte("abc")}
	stub.mu.Unlock()
	if _, err := backend.IncrBy(key, 1); err == nil {
		t.Fatalf("expected non-integer error")
	}

	stub.mu.Lock()
	stub.responses[string(key)] = &pb.GetResponse{Value: []byte(strconv.FormatInt(math.MaxInt64, 10))}
	stub.mu.Unlock()
	if _, err := backend.IncrBy(key, 1); err == nil {
		t.Fatalf("expected overflow error")
	}
}

func TestRaftBackendMGetAndExists(t *testing.T) {
	storeAddr, stub, stopStore := startStubTinyKv(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	backend, err := newRaftBackend(cfgPath, "", "host")
	if err != nil {
		t.Fatalf("new raft backend: %v", err)
	}
	defer func() {
		_ = backend.Close()
	}()

	key1 := []byte("k1")
	key2 := []byte("k2")
	key3 := []byte("k3")
	expireAt := uint64(time.Now().Add(time.Hour).Unix())
	ttlBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBuf, expireAt)

	stub.mu.Lock()
	stub.responses = map[string]*pb.GetResponse{
		string(key1):                      {Value: []byte("v1")},
		string(key2):                      {Value: []byte("v2")},
		string(ttlMetaKey(key2)):          {Value: ttlBuf},
		string(ttlMetaKey(key3)):          {Value: ttlBuf},
		string(append([]byte{}, key3...)): {NotFound: true},
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
	if !vals[1].Found || vals[1].ExpiresAt != expireAt {
		t.Fatalf("expected k2 ttl %d, got %+v", expireAt, vals[1])
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
	storeAddr, stub, stopStore := startStubTinyKv(t)
	defer stopStore()

	cfgPath := writeBackendConfig(t, storeAddr)
	backend, err := newRaftBackend(cfgPath, "", "host")
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
	stub.responses = map[string]*pb.GetResponse{
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

func TestDecodeTTLFromResponse(t *testing.T) {
	if got := decodeTTLFromResponse(nil); got != 0 {
		t.Fatalf("expected 0 for nil response")
	}
	if got := decodeTTLFromResponse(&pb.GetResponse{NotFound: true}); got != 0 {
		t.Fatalf("expected 0 for not found")
	}
	if got := decodeTTLFromResponse(&pb.GetResponse{Value: []byte("short")}); got != 0 {
		t.Fatalf("expected 0 for short ttl")
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 123)
	if got := decodeTTLFromResponse(&pb.GetResponse{Value: buf}); got != 123 {
		t.Fatalf("expected 123, got %d", got)
	}
}

func TestUniqueKeys(t *testing.T) {
	keys := uniqueKeys([][]byte{
		[]byte("a"),
		[]byte("a"),
		nil,
		[]byte("b"),
		[]byte(""),
	})
	if len(keys) != 2 {
		t.Fatalf("expected 2 unique keys, got %d", len(keys))
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
	batchGet    map[string]*pb.GetResponse
	batchGetErr error
	batchGetFn  func(keys [][]byte) (map[string]*pb.GetResponse, error) // Custom BatchGet logic
	mutateErr   error
	mutateFn    func() error
	mutateCalls int
	checkResp   *pb.CheckTxnStatusResponse
	checkErr    error
	checkCalls  int // Track CheckTxnStatus call count
	resolveResp uint64
	resolveErr  error
	resolveKeys [][]byte
}

func (s *stubRaftClient) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*pb.GetResponse, error) {
	if s.batchGetFn != nil {
		return s.batchGetFn(keys)
	}
	if s.batchGetErr != nil {
		return nil, s.batchGetErr
	}
	out := make(map[string]*pb.GetResponse, len(keys))
	for _, key := range keys {
		k := string(key)
		if resp, ok := s.batchGet[k]; ok {
			out[k] = resp
			continue
		}
		out[k] = &pb.GetResponse{NotFound: true}
	}
	return out, nil
}

func (s *stubRaftClient) Mutate(ctx context.Context, primary []byte, mutations []*pb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	s.mutateCalls++
	if s.mutateFn != nil {
		return s.mutateFn()
	}
	if s.mutateErr != nil {
		return s.mutateErr
	}
	return nil
}

func (s *stubRaftClient) CheckTxnStatus(ctx context.Context, primary []byte, lockVersion, currentTS uint64) (*pb.CheckTxnStatusResponse, error) {
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

func TestLocalOracleReserveZero(t *testing.T) {
	var oracle localOracle
	_, err := oracle.Reserve(0)
	require.Error(t, err)
}

func TestHTTPTSOReserveErrors(t *testing.T) {
	tso := newHTTPTSO("http://example")
	_, err := tso.Reserve(0)
	require.Error(t, err)

	tso = &httpTSO{url: "http://bad host", client: &http.Client{}}
	_, err = tso.Reserve(1)
	require.Error(t, err)

	tso = &httpTSO{
		url: "http://example",
		client: &http.Client{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("boom")
			}),
		},
	}
	_, err = tso.Reserve(1)
	require.Error(t, err)

	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer statusServer.Close()
	tso = newHTTPTSO(statusServer.URL)
	_, err = tso.Reserve(1)
	require.Error(t, err)

	badJSONServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{bad-json"))
	}))
	defer badJSONServer.Close()
	tso = newHTTPTSO(badJSONServer.URL)
	_, err = tso.Reserve(1)
	require.Error(t, err)

	countServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"timestamp": uint64(1),
			"count":     uint64(1),
		})
	}))
	defer countServer.Close()
	tso = newHTTPTSO(countServer.URL)
	_, err = tso.Reserve(2)
	require.Error(t, err)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestNewRaftBackendErrors(t *testing.T) {
	_, err := newRaftBackend(filepath.Join(t.TempDir(), "missing.json"), "", "host")
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
	_, err = newRaftBackend(cfgPath, "", "host")
	require.Error(t, err)

	storeAddr, _, stopStore := startStubTinyKv(t)
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
		TSO: &config.TSO{
			ListenAddr: "127.0.0.1:9494",
		},
	}
	raw, err = json.Marshal(cfg)
	require.NoError(t, err)
	cfgPath = filepath.Join(dir, "cfg2.json")
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))
	backend, err := newRaftBackend(cfgPath, "", "host")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()
	httpTSO, ok := backend.ts.(*httpTSO)
	require.True(t, ok)
	require.Equal(t, "http://127.0.0.1:9494", httpTSO.url)
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

	stub.batchGet = map[string]*pb.GetResponse{
		"key": {Value: []byte("v")},
	}
	ok, err := backend.Set(setArgs{Key: []byte("key"), Value: []byte("v2"), NX: true})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, stub.mutateCalls)

	stub.batchGet = map[string]*pb.GetResponse{}
	ok, err = backend.Set(setArgs{Key: []byte("missing"), Value: []byte("v"), XX: true})
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = backend.Set(setArgs{Key: []byte("ttl"), Value: []byte("v"), ExpireAt: 123})
	require.NoError(t, err)
	require.True(t, ok)

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
	stub.batchGet = map[string]*pb.GetResponse{
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
	stub.batchGet = map[string]*pb.GetResponse{
		string(ttlMetaKey([]byte("k"))): {Value: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
	}
	stub.mutateErr = errors.New("mutate")
	_, err = backend.MGet([][]byte{[]byte("k")})
	require.Error(t, err)
}

func TestRaftBackendMSetBranches(t *testing.T) {
	backend, stub, _ := newStubBackend()
	require.NoError(t, backend.MSet(nil))
	require.Error(t, backend.MSet([][2][]byte{{nil, []byte("v")}}))

	stub.mutateErr = errors.New("mutate")
	require.Error(t, backend.MSet([][2][]byte{{[]byte("k"), []byte("v")}}))
}

func TestRaftBackendExists(t *testing.T) {
	backend, stub, ts := newStubBackend()
	stub.batchGet = map[string]*pb.GetResponse{
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
	stub.batchGet = map[string]*pb.GetResponse{
		"k": {Value: []byte("abc")},
	}
	_, err = backend.IncrBy([]byte("k"), 1)
	require.ErrorIs(t, err, errNotInteger)

	stub.batchGet = map[string]*pb.GetResponse{
		"k": {Value: fmt.Appendf(nil, "%d", math.MaxInt64)},
	}
	_, err = backend.IncrBy([]byte("k"), 1)
	require.ErrorIs(t, err, errOverflow)

	stub.batchGet = map[string]*pb.GetResponse{
		"k": {Value: fmt.Appendf(nil, "%d", math.MinInt64)},
	}
	_, err = backend.IncrBy([]byte("k"), -1)
	require.ErrorIs(t, err, errOverflow)
}

func TestBuildValueAtVersionBranches(t *testing.T) {
	backend, stub, _ := newStubBackend()
	key := []byte("k1")
	ttlBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBuf, uint64(time.Now().Unix()+100))

	val, err := backend.buildValueAtVersion(key, &pb.GetResponse{NotFound: true}, &pb.GetResponse{Value: ttlBuf})
	require.NoError(t, err)
	require.False(t, val.Found)
	require.NotZero(t, stub.mutateCalls)

	stub.mutateCalls = 0
	expiredBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(expiredBuf, uint64(time.Now().Add(-time.Hour).Unix()))
	val, err = backend.buildValueAtVersion(key, &pb.GetResponse{Value: []byte("v")}, &pb.GetResponse{Value: expiredBuf})
	require.NoError(t, err)
	require.False(t, val.Found)
	require.NotZero(t, stub.mutateCalls)

	stub.mutateCalls = 0
	val, err = backend.buildValueAtVersion(key, &pb.GetResponse{Value: []byte("v")}, &pb.GetResponse{Value: ttlBuf})
	require.NoError(t, err)
	require.True(t, val.Found)
	require.Equal(t, []byte("v"), val.Value)
}

func TestMutatePaths(t *testing.T) {
	backend, stub, ts := newStubBackend()
	require.NoError(t, backend.mutate([]byte("k")))

	ts.err = errors.New("reserve")
	err := backend.mutate([]byte("k"), &pb.Mutation{Op: pb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)

	ts.err = nil
	stub.mutateErr = errors.New("boom")
	err = backend.mutate([]byte("k"), &pb.Mutation{Op: pb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)

	conflict := &client.KeyConflictError{Errors: []*pb.KeyError{{Locked: &pb.Locked{
		Key:         []byte("k"),
		PrimaryLock: []byte("k"),
		LockVersion: 1,
	}}}}
	stub.mutateErr = conflict
	stub.checkResp = &pb.CheckTxnStatusResponse{Action: pb.CheckTxnStatusAction_CheckTxnStatusNoAction}
	err = backend.mutate([]byte("k"), &pb.Mutation{Op: pb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)

	stub.mutateErr = nil
	stub.mutateFn = func() error { return conflict }
	stub.checkResp = &pb.CheckTxnStatusResponse{Action: pb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback}
	stub.resolveErr = nil
	err = backend.mutate([]byte("k"), &pb.Mutation{Op: pb.Mutation_Put, Key: []byte("k")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "retries exhausted")
}

func TestResolveKeyConflictsAndLocks(t *testing.T) {
	backend, stub, _ := newStubBackend()
	require.False(t, backend.resolveKeyConflicts(nil))
	require.False(t, backend.resolveKeyConflicts(&client.KeyConflictError{}))

	conflicts := &client.KeyConflictError{Errors: []*pb.KeyError{nil, {}}}
	require.True(t, backend.resolveKeyConflicts(conflicts))

	lock := &pb.Locked{
		PrimaryLock: []byte("p"),
		Key:         ttlMetaKey([]byte("k")),
		LockVersion: 1,
	}
	stub.checkErr = errors.New("check")
	require.False(t, backend.resolveSingleLock(lock))

	stub.checkErr = nil
	stub.checkResp = &pb.CheckTxnStatusResponse{CommitVersion: 1}
	// The primary is committed, the lock should be resolved successfully
	require.True(t, backend.resolveSingleLock(lock))

	stub.checkResp = &pb.CheckTxnStatusResponse{Action: pb.CheckTxnStatusAction_CheckTxnStatusNoAction}
	require.False(t, backend.resolveSingleLock(lock))

	stub.checkResp = &pb.CheckTxnStatusResponse{Action: pb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback}
	stub.resolveErr = errors.New("resolve")
	require.False(t, backend.resolveSingleLock(lock))

	stub.resolveErr = nil
	require.True(t, backend.resolveSingleLock(lock))
	require.NotEmpty(t, stub.resolveKeys)
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
			Errors: []*pb.KeyError{
				{
					Locked: &pb.Locked{
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
	stub.checkResp = &pb.CheckTxnStatusResponse{
		CommitVersion: commitVersion,
	}

	// Execute mutate - this will encounter the lock and try to resolve
	err := backend.mutate(primaryKey, &pb.Mutation{
		Op:    pb.Mutation_Put,
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
	stub.batchGetFn = func(keys [][]byte) (map[string]*pb.GetResponse, error) {
		callCount++
		for _, k := range keys {
			if bytes.Equal(k, key) && !stub.keyResolved(k) {
				return nil, &client.KeyConflictError{
					Errors: []*pb.KeyError{{
						Locked: &pb.Locked{
							PrimaryLock: primaryKey,
							Key:         k,
							LockVersion: lockVersion,
							LockTtl:     3000,
						},
					}},
				}
			}
		}
		out := make(map[string]*pb.GetResponse)
		for _, k := range keys {
			if bytes.Equal(k, key) {
				out[string(k)] = &pb.GetResponse{Value: []byte("committed-value")}
			} else {
				out[string(k)] = &pb.GetResponse{NotFound: true}
			}
		}
		return out, nil
	}

	// CheckTxnStatus returns: primary is already committed
	stub.checkResp = &pb.CheckTxnStatusResponse{CommitVersion: commitVersion}

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
	stub.batchGetFn = func(keys [][]byte) (map[string]*pb.GetResponse, error) {
		return nil, &client.KeyConflictError{
			Errors: []*pb.KeyError{{
				Locked: &pb.Locked{
					PrimaryLock: primaryKey,
					Key:         key,
					LockVersion: lockVersion,
					LockTtl:     3000,
				},
			}},
		}
	}

	// CheckTxnStatus returns: primary is still alive (no action needed)
	stub.checkResp = &pb.CheckTxnStatusResponse{
		Action: pb.CheckTxnStatusAction_CheckTxnStatusNoAction,
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

	stub.batchGetFn = func(keys [][]byte) (map[string]*pb.GetResponse, error) {
		for _, k := range keys {
			if bytes.Equal(k, secondaryKey) && !stub.keyResolved(k) {
				return nil, &client.KeyConflictError{
					Errors: []*pb.KeyError{{
						Locked: &pb.Locked{
							PrimaryLock: primaryKey,
							Key:         k,
							LockVersion: lockVersion,
							LockTtl:     3000,
						},
					}},
				}
			}
		}
		out := make(map[string]*pb.GetResponse)
		for _, k := range keys {
			out[string(k)] = &pb.GetResponse{NotFound: true}
		}
		return out, nil
	}

	// CheckTxnStatus returns: primary lock does not exist (rolled back)
	stub.checkResp = &pb.CheckTxnStatusResponse{
		Action: pb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback,
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

	stub.batchGetFn = func(keys [][]byte) (map[string]*pb.GetResponse, error) {
		for _, k := range keys {
			if bytes.Equal(k, secondaryKey) && !stub.keyResolved(k) {
				return nil, &client.KeyConflictError{
					Errors: []*pb.KeyError{{
						Locked: &pb.Locked{
							PrimaryLock: primaryKey,
							Key:         k,
							LockVersion: lockVersion,
							LockTtl:     1, // Very short TTL
						},
					}},
				}
			}
		}
		out := make(map[string]*pb.GetResponse)
		for _, k := range keys {
			out[string(k)] = &pb.GetResponse{NotFound: true}
		}
		return out, nil
	}

	// CheckTxnStatus returns: TTL expired, should rollback
	stub.checkResp = &pb.CheckTxnStatusResponse{
		Action: pb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
	}

	val, err := backend.Get(secondaryKey)
	require.NoError(t, err)
	require.False(t, val.Found, "value should not be found after TTL-expired rollback")
	require.True(t, stub.checkCalls > 0, "CheckTxnStatus should be called")
	require.True(t, len(stub.resolveKeys) > 0, "ResolveLocks should be called when TTL expired")
}
