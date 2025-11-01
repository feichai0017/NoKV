package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
	for i := range responses {
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

	cfg := raftConfigFile{
		MaxRetries: 3,
		Stores: []raftStoreConfig{
			{
				StoreID:    1,
				Addr:       "127.0.0.1:1", // intentionally invalid so docker scope must be used
				DockerAddr: storeAddr,
			},
		},
		Regions: []raftRegionConfig{
			{
				ID:       1,
				StartKey: "a",
				EndKey:   "-",
				Epoch: raftRegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []raftRegionPeer{
					{StoreID: 1, PeerID: 101},
				},
				LeaderStoreID: 1,
			},
		},
		TSO: &tsoConfig{
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

	cfg := raftConfigFile{
		Stores: []raftStoreConfig{
			{
				StoreID: 1,
				Addr:    storeAddr,
			},
		},
		Regions: []raftRegionConfig{
			{
				ID: 1,
				Epoch: raftRegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []raftRegionPeer{
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

	cfg := raftConfigFile{
		MaxRetries: 3,
		Stores: []raftStoreConfig{
			{
				StoreID: 1,
				Addr:    storeAddr,
			},
		},
		Regions: []raftRegionConfig{
			{
				ID: 1,
				Epoch: raftRegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []raftRegionPeer{
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
