package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = backend.client.Get(ctx, []byte("key"), 1)
	if err == nil {
		t.Fatalf("expected unimplemented error from stub server")
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
