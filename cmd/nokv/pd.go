package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/config"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"
	"github.com/feichai0017/NoKV/pd/tso"
	"google.golang.org/grpc"
)

var pdNotifyContext = signal.NotifyContext
var pdListen = net.Listen

func runPDCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("pd", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:2379", "listen address for PD-lite gRPC service")
	idStart := fs.Uint64("id-start", 1, "initial ID allocator value")
	tsStart := fs.Uint64("ts-start", 1, "initial TSO value")
	rootMode := fs.String("root-mode", "local", "metadata root mode: local|replicated")
	rootNodeID := fs.Uint64("root-node-id", 1, "metadata root node id for replicated mode")
	rootTransportAddr := fs.String("root-transport-addr", "", "metadata root raft transport address for replicated mode")
	rootRefresh := fs.Duration("root-refresh", 200*time.Millisecond, "refresh interval for replicated rooted PD state")
	workdir := fs.String("workdir", "", "optional PD local state directory for persisting allocator and region catalog")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve pd workdir")
	scope := fs.String("scope", "host", "scope for config-resolved pd workdir: host|docker")
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
			return fmt.Errorf("invalid pd scope %q (expected host|docker)", *scope)
		}
		cfg, err := config.LoadFile(strings.TrimSpace(*configPath))
		if err != nil {
			return fmt.Errorf("pd load config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("pd validate config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if !flagPassed(fs, "addr") {
			if resolved := cfg.ResolvePDAddr(scopeNorm); resolved != "" {
				*addr = resolved
			}
		}
		if !flagPassed(fs, "workdir") {
			if resolved := cfg.ResolvePDWorkDir(scopeNorm); resolved != "" {
				*workdir = resolved
			}
		}
	}
	rootModeValue := strings.ToLower(strings.TrimSpace(*rootMode))
	switch rootModeValue {
	case "", "local", "replicated":
	default:
		return fmt.Errorf("invalid root mode %q (expected local|replicated)", *rootMode)
	}

	lis, err := pdListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("pd listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	cluster := core.NewCluster()
	var (
		store         pdstorage.Store
		rootStore     *pdstorage.RootStore
		workdirPath   string
		loadedRegions int
	)
	workdirPath = strings.TrimSpace(*workdir)
	switch rootModeValue {
	case "", "local":
		if workdirPath == "" {
			break
		}
		rootStore, err = pdstorage.OpenRootLocalStore(workdirPath)
		if err != nil {
			return fmt.Errorf("pd open metadata root %q: %w", workdirPath, err)
		}
		defer func() { _ = rootStore.Close() }()
		bootstrap, err := pdstorage.Bootstrap(rootStore, cluster, *idStart, *tsStart)
		if err != nil {
			return fmt.Errorf("pd bootstrap from %q: %w", workdirPath, err)
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
		rootStore, openErr := pdstorage.OpenRootReplicatedStore(pdstorage.ReplicatedRootConfig{
			WorkDir:       workdirPath,
			NodeID:        *rootNodeID,
			TransportAddr: strings.TrimSpace(*rootTransportAddr),
			PeerAddrs:     rootPeers,
		})
		if openErr != nil {
			return fmt.Errorf("pd open replicated metadata root: %w", openErr)
		}
		defer func() { _ = rootStore.Close() }()
		bootstrap, err := pdstorage.Bootstrap(rootStore, cluster, *idStart, *tsStart)
		if err != nil {
			return fmt.Errorf("pd bootstrap from replicated metadata root: %w", err)
		}
		*idStart, *tsStart = bootstrap.IDStart, bootstrap.TSStart
		loadedRegions = bootstrap.LoadedRegions
		store = rootStore
	}

	ids := core.NewIDAllocator(*idStart)
	tsAlloc := tso.NewAllocator(*tsStart)
	svc := pdserver.NewService(cluster, ids, tsAlloc)
	if store != nil {
		svc.SetStorage(store)
	}

	grpcServer := grpc.NewServer()
	pdpb.RegisterPDServer(grpcServer, svc)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()
	metricsLn, err := startExpvarServer(*metricsAddr)
	if err != nil {
		return fmt.Errorf("start pd metrics endpoint: %w", err)
	}
	if metricsLn != nil {
		defer func() { _ = metricsLn.Close() }()
	}

	if store != nil {
		if rootModeValue == "replicated" {
			_, _ = fmt.Fprintf(w, "PD restored %d region(s) from replicated metadata root\n", loadedRegions)
		} else {
			_, _ = fmt.Fprintf(w, "PD restored %d region(s) from metadata root: %s\n", loadedRegions, workdirPath)
		}
		_, _ = fmt.Fprintf(w, "PD allocator starts: id=%d ts=%d\n", *idStart, *tsStart)
		_, _ = fmt.Fprintf(w, "PD metadata root mode: %s\n", rootModeValue)
	}
	_, _ = fmt.Fprintf(w, "PD-lite service listening on %s\n", lis.Addr().String())
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "PD metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}
	ctx, cancel := pdNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if refresher, ok := store.(pdstorage.Refresher); ok && rootModeValue == "replicated" && rootStore != nil {
		ticker := time.NewTicker(*rootRefresh)
		defer ticker.Stop()
		go func() {
			var last rootstate.Cursor
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					next, err := rootStore.WaitForChange(last, *rootRefresh)
					if err == nil && rootstate.CursorAfter(next, last) {
						_ = svc.RefreshFromStorage()
						last = next
						continue
					}
					_ = refresher.Refresh()
					_ = svc.RefreshFromStorage()
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
