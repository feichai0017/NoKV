package controlplane

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	benchHelperEnv      = "NOKV_CONTROL_PLANE_BENCH_HELPER"
	benchHelperRootMode = "root-server"
	benchHelperEtcdMode = "etcd-server"
)

func TestControlPlaneBenchHelperProcess(t *testing.T) {
	if os.Getenv(benchHelperEnv) != "1" {
		t.Skip("control-plane benchmark helper only")
	}

	args := helperProcessArgs()
	if len(args) == 0 {
		os.Exit(2)
	}
	switch args[0] {
	case benchHelperRootMode:
		runRootHelperProcess(args[1:])
	case benchHelperEtcdMode:
		runEtcdHelperProcess(args[1:])
	default:
		os.Exit(2)
	}
}

type rootFenceAllocator struct {
	store      *coordstorage.RootStore
	current    uint64
	windowHigh uint64
	tsFence    uint64
	windowSize uint64
}

func (a *rootFenceAllocator) Reserve(ctx context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, fmt.Errorf("reserve count must be >= 1")
	}
	next := a.current + count - 1
	if next > a.windowHigh {
		windowHigh := a.current + maxUint64(a.windowSize, count) - 1
		if err := a.store.SaveAllocatorState(ctx, windowHigh, a.tsFence); err != nil {
			return 0, err
		}
		a.windowHigh = windowHigh
	}
	first := a.current
	a.current += count
	return first, nil
}

func etcdCurrentFence(resp *clientv3.GetResponse) (uint64, int64) {
	if len(resp.Kvs) == 0 {
		return 0, 0
	}
	current, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, resp.Kvs[0].ModRevision
	}
	return current, resp.Kvs[0].ModRevision
}

func helperProcessArgs() []string {
	for i, arg := range os.Args {
		if arg == "--" && i+1 < len(os.Args) {
			return os.Args[i+1:]
		}
	}
	return nil
}

func startBenchHelperProcess(tb testing.TB, mode string, args ...string) *exec.Cmd {
	tb.Helper()
	cmdArgs := []string{"-test.run=TestControlPlaneBenchHelperProcess", "--", mode}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), benchHelperEnv+"=1")
	require.NoError(tb, cmd.Start())
	return cmd
}

func stopBenchHelperProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func waitForGRPCReady(tb testing.TB, addr string) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for gRPC helper at %s", addr)
}

func waitForEtcdReady(tb testing.TB, endpoint string) {
	tb.Helper()
	u, err := url.Parse(endpoint)
	require.NoError(tb, err)
	waitForTCPOpen(tb, u.Host)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: 200 * time.Millisecond,
			Logger:      zap.NewNop(),
		})
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, getErr := client.Get(ctx, "/")
			cancel()
			_ = client.Close()
			if getErr == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for etcd helper at %s", endpoint)
}

func waitForTCPOpen(tb testing.TB, addr string) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for tcp listener at %s", addr)
}

func runRootHelperProcess(args []string) {
	if len(args) != 2 {
		os.Exit(2)
	}
	addr, workdir := args[0], args[1]
	backend, err := rootlocal.Open(workdir, nil)
	if err != nil {
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		os.Exit(1)
	}
	server := grpc.NewServer()
	metapb.RegisterMetadataRootServer(server, rootremote.NewService(backend))
	go func() { _ = server.Serve(listener) }()
	waitForTermination(func() {
		server.GracefulStop()
		_ = listener.Close()
		_ = backend.Close()
	})
}

func runEtcdHelperProcess(args []string) {
	if len(args) != 3 {
		os.Exit(2)
	}
	clientAddr, peerAddr, workdir := args[0], args[1], args[2]
	clientURL, err := url.Parse(clientAddr)
	if err != nil {
		os.Exit(1)
	}
	peerURL, err := url.Parse(peerAddr)
	if err != nil {
		os.Exit(1)
	}
	cfg := embed.NewConfig()
	cfg.Name = "bench-node"
	cfg.Dir = workdir
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{"/dev/null"}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		os.Exit(1)
	}
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		server.Close()
		os.Exit(1)
	}
	waitForTermination(server.Close)
}

func waitForTermination(stop func()) {
	var once sync.Once
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	<-signals
	once.Do(stop)
}

func mustFreeAddr(tb testing.TB) string {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	addr := listener.Addr().String()
	require.NoError(tb, listener.Close())
	return addr
}

func mustFreeURL(tb testing.TB, scheme string) url.URL {
	tb.Helper()
	addr := mustFreeAddr(tb)
	u, err := url.Parse(fmt.Sprintf("%s://%s", scheme, addr))
	require.NoError(tb, err)
	return *u
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func TestRootFenceAllocatorReserveAdvancesWindow(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)

	store, err := coordstorage.OpenRootStore(backend)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	a := &rootFenceAllocator{
		store:      store,
		current:    1,
		windowHigh: 0,
		tsFence:    0,
		windowSize: 4,
	}

	first, err := a.Reserve(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshot.Allocator.IDCurrent)

	first, err = a.Reserve(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), first)

	snapshot, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshot.Allocator.IDCurrent)

	first, err = a.Reserve(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), first)

	snapshot, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(6), snapshot.Allocator.IDCurrent)
}

func TestRootFenceAllocatorReserveUsesBatchWhenBatchExceedsWindow(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)

	store, err := coordstorage.OpenRootStore(backend)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	a := &rootFenceAllocator{
		store:      store,
		current:    1,
		windowHigh: 0,
		tsFence:    0,
		windowSize: 2,
	}

	first, err := a.Reserve(context.Background(), 5)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(5), snapshot.Allocator.IDCurrent)
}

func TestEtcdCurrentFenceParsesValue(t *testing.T) {
	current, modRev := etcdCurrentFence(&clientv3.GetResponse{})
	require.Equal(t, uint64(0), current)
	require.Equal(t, int64(0), modRev)
}
