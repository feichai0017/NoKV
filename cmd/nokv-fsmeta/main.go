package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/config"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	metricspkg "github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var exit = os.Exit

var fatalf = func(format string, args ...any) {
	log.Printf(format, args...)
	exit(1)
}

var listen = net.Listen
var signalNotify = signal.Notify
var openExecutor = newExecutorFromConfig

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:8090", "listen address for FSMetadata gRPC server")
		raftConfig  = flag.String("raft-config", "", "JSON config describing raftstore cluster endpoints")
		coordAddr   = flag.String("coordinator-addr", "", "optional coordinator gRPC endpoint override; defaults to config.coordinator")
		addrScope   = flag.String("addr-scope", "host", "store/coordinator address scope to use (host|docker)")
		metricsAddr = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	)
	flag.Parse()

	if strings.TrimSpace(*raftConfig) == "" {
		fatalf("raft-config is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor, closeExecutor, err := openExecutor(ctx, *raftConfig, *coordAddr, *addrScope)
	if err != nil {
		fatalf("open fsmeta executor: %v", err)
	}
	defer func() {
		if closeExecutor != nil {
			if err := closeExecutor(); err != nil {
				log.Printf("close fsmeta executor: %v", err)
			}
		}
	}()

	ln, err := listen("tcp", *addr)
	if err != nil {
		fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	grpcServer := grpc.NewServer()
	fsmetaserver.Register(grpcServer, executor)
	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(ln)
	}()

	if *metricsAddr != "" {
		metricsLn, err := metricspkg.StartExpvarServer(*metricsAddr)
		if err != nil {
			fatalf("start metrics endpoint: %v", err)
		}
		defer func() {
			if metricsLn != nil {
				_ = metricsLn.Close()
			}
		}()
		log.Printf("expvar metrics listening on http://%s/debug/vars", metricsLn.Addr().String())
	}

	log.Printf("NoKV FSMetadata server listening on %s", ln.Addr().String())

	sigCh := make(chan os.Signal, 1)
	signalNotify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
	case err := <-errCh:
		if err != nil {
			log.Printf("serve loop exited: %v", err)
		}
	}
	cancel()
	grpcServer.GracefulStop()
}

type closeFunc func() error

func newExecutorFromConfig(ctx context.Context, cfgPath, coordAddr, addrScope string) (*fsmetaexec.Executor, closeFunc, error) {
	cfgFile, err := config.LoadFile(cfgPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read config: %w", err)
	}
	if err := cfgFile.Validate(); err != nil {
		return nil, nil, fmt.Errorf("config invalid: %w", err)
	}
	scope := strings.ToLower(strings.TrimSpace(addrScope))
	if scope == "" {
		scope = "host"
	}
	if scope != "host" && scope != "docker" {
		return nil, nil, fmt.Errorf("unknown addr-scope %q (expected host|docker)", addrScope)
	}

	stores := make([]client.StoreEndpoint, 0, len(cfgFile.Stores))
	for _, st := range cfgFile.Stores {
		addr := strings.TrimSpace(st.Addr)
		if scope == "docker" && strings.TrimSpace(st.DockerAddr) != "" {
			addr = strings.TrimSpace(st.DockerAddr)
		}
		if addr == "" {
			return nil, nil, fmt.Errorf("store %d address missing for scope %q", st.StoreID, scope)
		}
		stores = append(stores, client.StoreEndpoint{StoreID: st.StoreID, Addr: addr})
	}
	if len(stores) == 0 {
		return nil, nil, fmt.Errorf("no stores configured")
	}

	coordAddr = strings.TrimSpace(coordAddr)
	if coordAddr == "" {
		coordAddr = cfgFile.ResolveCoordinatorAddr(scope)
	}
	if coordAddr == "" {
		return nil, nil, fmt.Errorf("coordinator-addr is required")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	coordRPC, err := coordclient.NewGRPCClient(dialCtx, coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("init coordinator client: %w", err)
	}
	kv, err := client.New(client.Config{
		Context:        ctx,
		Stores:         stores,
		RegionResolver: coordRPC,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: client.RetryPolicy{
			MaxAttempts: cfgFile.MaxRetries,
		},
	})
	if err != nil {
		_ = coordRPC.Close()
		return nil, nil, fmt.Errorf("init raftstore client: %w", err)
	}
	runner, err := fsmetaexec.NewRaftstoreRunner(kv, coordRPC)
	if err != nil {
		_ = kv.Close()
		return nil, nil, fmt.Errorf("init fsmeta runner: %w", err)
	}
	executor, err := fsmetaexec.New(runner)
	if err != nil {
		_ = kv.Close()
		return nil, nil, fmt.Errorf("init fsmeta executor: %w", err)
	}
	return executor, kv.Close, nil
}
