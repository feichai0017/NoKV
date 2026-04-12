package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"google.golang.org/grpc"
)

var coordinatorNotifyContext = signal.NotifyContext
var coordinatorListen = net.Listen

func runCoordinatorCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:2379", "listen address for coordinator gRPC service")
	idStart := fs.Uint64("id-start", 1, "initial ID allocator value")
	tsStart := fs.Uint64("ts-start", 1, "initial TSO value")
	rootMode := fs.String("root-mode", "local", "metadata root mode: local|replicated|remote")
	rootNodeID := fs.Uint64("root-node-id", 1, "metadata root node id for replicated mode")
	rootTransportAddr := fs.String("root-transport-addr", "", "metadata root raft transport address for replicated mode")
	rootRefresh := fs.Duration("root-refresh", 200*time.Millisecond, "refresh interval for replicated rooted coordinator state")
	coordinatorID := fs.String("coordinator-id", "", "stable coordinator owner id for lease-gated control plane")
	leaseTTL := fs.Duration("lease-ttl", 10*time.Second, "coordinator lease ttl when --coordinator-id is set")
	leaseRenewBefore := fs.Duration("lease-renew-before", 3*time.Second, "renew/campaign before lease expiry when --coordinator-id is set")
	workdir := fs.String("workdir", "", "optional coordinator local state directory for persisting allocator and region catalog")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve coordinator workdir")
	scope := fs.String("scope", "host", "scope for config-resolved coordinator workdir: host|docker")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	var rootPeerFlags []string
	fs.Func("root-peer", "replicated metadata root peer mapping in the form nodeID=address (repeatable, exactly 3)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("root-peer value cannot be empty")
		}
		rootPeerFlags = append(rootPeerFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*configPath) != "" {
		scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
		if scopeNorm != "host" && scopeNorm != "docker" {
			return fmt.Errorf("invalid coordinator scope %q (expected host|docker)", *scope)
		}
		cfg, err := config.LoadFile(strings.TrimSpace(*configPath))
		if err != nil {
			return fmt.Errorf("coordinator load config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("coordinator validate config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if !flagPassed(fs, "addr") {
			if resolved := cfg.ResolveCoordinatorAddr(scopeNorm); resolved != "" {
				*addr = resolved
			}
		}
		if !flagPassed(fs, "workdir") {
			if resolved := cfg.ResolveCoordinatorWorkDir(scopeNorm); resolved != "" {
				*workdir = resolved
			}
		}
	}
	rootModeValue := strings.ToLower(strings.TrimSpace(*rootMode))
	switch rootModeValue {
	case "", "local", "replicated", "remote":
	default:
		return fmt.Errorf("invalid root mode %q (expected local|replicated|remote)", *rootMode)
	}
	coordinatorIDValue := strings.TrimSpace(*coordinatorID)
	if rootModeValue == "remote" && coordinatorIDValue == "" {
		return fmt.Errorf("coordinator remote root mode requires --coordinator-id")
	}
	if coordinatorIDValue == "" && (flagPassed(fs, "lease-ttl") || flagPassed(fs, "lease-renew-before")) {
		return fmt.Errorf("coordinator lease flags require --coordinator-id")
	}

	lis, err := coordinatorListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("coordinator listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	cluster := catalog.NewCluster()
	var (
		store         coordstorage.RootStorage
		rootStore     *coordstorage.RootStore
		workdirPath   string
		loadedRegions int
	)
	workdirPath = strings.TrimSpace(*workdir)
	switch rootModeValue {
	case "", "local":
		if workdirPath == "" {
			break
		}
		rootStore, err = coordstorage.OpenRootLocalStore(workdirPath)
		if err != nil {
			return fmt.Errorf("coordinator open metadata root %q: %w", workdirPath, err)
		}
		defer func() { _ = rootStore.Close() }()
		bootstrap, err := coordstorage.Bootstrap(rootStore, cluster.PublishRegionDescriptor, *idStart, *tsStart)
		if err != nil {
			return fmt.Errorf("coordinator bootstrap from %q: %w", workdirPath, err)
		}
		*idStart, *tsStart = bootstrap.IDStart, bootstrap.TSStart
		loadedRegions = bootstrap.LoadedRegions
		store = rootStore
	case "replicated":
		rootPeers, err := parseReplicatedRootPeers(rootPeerFlags)
		if err != nil {
			return err
		}
		workdirPath = strings.TrimSpace(*workdir)
		rootStore, openErr := coordstorage.OpenRootReplicatedStore(coordstorage.ReplicatedRootConfig{
			WorkDir:       workdirPath,
			NodeID:        *rootNodeID,
			TransportAddr: strings.TrimSpace(*rootTransportAddr),
			PeerAddrs:     rootPeers,
		})
		if openErr != nil {
			return fmt.Errorf("coordinator open replicated metadata root: %w", openErr)
		}
		defer func() { _ = rootStore.Close() }()
		bootstrap, err := coordstorage.Bootstrap(rootStore, cluster.PublishRegionDescriptor, *idStart, *tsStart)
		if err != nil {
			return fmt.Errorf("coordinator bootstrap from replicated metadata root: %w", err)
		}
		*idStart, *tsStart = bootstrap.IDStart, bootstrap.TSStart
		loadedRegions = bootstrap.LoadedRegions
		store = rootStore
	case "remote":
		rootPeers, err := parseReplicatedRootPeers(rootPeerFlags)
		if err != nil {
			return err
		}
		rootStore, err = coordstorage.OpenRootRemoteStore(coordstorage.RemoteRootConfig{
			Targets: rootPeers,
		})
		if err != nil {
			return fmt.Errorf("coordinator open remote metadata root: %w", err)
		}
		defer func() { _ = rootStore.Close() }()
		bootstrap, err := coordstorage.Bootstrap(rootStore, cluster.PublishRegionDescriptor, *idStart, *tsStart)
		if err != nil {
			return fmt.Errorf("coordinator bootstrap from remote metadata root: %w", err)
		}
		*idStart, *tsStart = bootstrap.IDStart, bootstrap.TSStart
		loadedRegions = bootstrap.LoadedRegions
		store = rootStore
	}

	ids := idalloc.NewIDAllocator(*idStart)
	tsAlloc := tso.NewAllocator(*tsStart)
	svc := coordserver.NewService(cluster, ids, tsAlloc, store)
	if coordinatorIDValue != "" {
		if store == nil {
			return fmt.Errorf("coordinator --coordinator-id requires rooted storage")
		}
		svc.ConfigureCoordinatorLease(coordinatorIDValue, *leaseTTL, *leaseRenewBefore)
	}
	installCoordinatorExpvar(rootModeValue, svc)

	grpcServer := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(grpcServer, svc)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()
	metricsLn, err := startExpvarServer(*metricsAddr)
	if err != nil {
		return fmt.Errorf("start coordinator metrics endpoint: %w", err)
	}
	if metricsLn != nil {
		defer func() { _ = metricsLn.Close() }()
	}

	if store != nil {
		if rootModeValue == "replicated" {
			_, _ = fmt.Fprintf(w, "Coordinator restored %d region(s) from replicated metadata root\n", loadedRegions)
		} else if rootModeValue == "remote" {
			_, _ = fmt.Fprintf(w, "Coordinator restored %d region(s) from remote metadata root\n", loadedRegions)
		} else {
			_, _ = fmt.Fprintf(w, "Coordinator restored %d region(s) from metadata root: %s\n", loadedRegions, workdirPath)
		}
		_, _ = fmt.Fprintf(w, "Coordinator allocator starts: id=%d ts=%d\n", *idStart, *tsStart)
		_, _ = fmt.Fprintf(w, "Coordinator metadata root mode: %s\n", rootModeValue)
		if coordinatorIDValue != "" {
			_, _ = fmt.Fprintf(w, "Coordinator lease owner: id=%s ttl=%s renew_before=%s\n", coordinatorIDValue, leaseTTL.String(), leaseRenewBefore.String())
		}
	}
	_, _ = fmt.Fprintf(w, "Coordinator service listening on %s\n", lis.Addr().String())
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "Coordinator metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}
	ctx, cancel := coordinatorNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if coordinatorIDValue != "" {
		go svc.RunCoordinatorLeaseLoop(ctx)
	}
	if (rootModeValue == "replicated" || rootModeValue == "remote") && rootStore != nil {
		go func() {
			subscription := rootStore.SubscribeTail(rootstorage.TailToken{})
			if subscription == nil {
				return
			}
			for {
				next, err := subscription.Next(ctx, *rootRefresh)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					continue
				}
				switch next.CatchUpAction() {
				case rootstorage.TailCatchUpRefreshState, rootstorage.TailCatchUpInstallBootstrap:
					if err := svc.ReloadFromStorage(); err != nil {
						continue
					}
					subscription.Acknowledge(next)
				case rootstorage.TailCatchUpAcknowledgeWindow:
					subscription.Acknowledge(next)
				}
			}
		}()
	}

	select {
	case serveErr := <-serveErrCh:
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return serveErr
		}
		return nil
	case <-ctx.Done():
		if coordinatorIDValue != "" {
			_ = svc.ReleaseCoordinatorLease()
		}
		grpcServer.GracefulStop()
		serveErr := <-serveErrCh
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return serveErr
		}
		return nil
	}
}

func parseReplicatedRootPeers(values []string) (map[uint64]string, error) {
	peers := make(map[uint64]string, len(values))
	for _, raw := range values {
		parts := strings.SplitN(strings.TrimSpace(raw), "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid -root-peer value %q (want nodeID=address)", raw)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid root peer id in %q: %w", raw, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("invalid root peer id in %q: must be > 0", raw)
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			return nil, fmt.Errorf("invalid -root-peer value %q (empty address)", raw)
		}
		if _, ok := peers[id]; ok {
			return nil, fmt.Errorf("duplicate root peer id %d", id)
		}
		peers[id] = addr
	}
	return peers, nil
}

func flagPassed(fs *flag.FlagSet, name string) bool {
	if fs == nil || name == "" {
		return false
	}
	passed := false
	fs.Visit(func(f *flag.Flag) {
		if f != nil && f.Name == name {
			passed = true
		}
	})
	return passed
}
