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
	rootCluster := fs.String("root-cluster", "1,2,3", "fixed metadata root cluster ids for replicated mode")
	rootRefresh := fs.Duration("root-refresh", 200*time.Millisecond, "refresh interval for replicated rooted PD state")
	workdir := fs.String("workdir", "", "optional PD local state directory for persisting allocator and region catalog")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve pd workdir")
	scope := fs.String("scope", "host", "scope for config-resolved pd workdir: host|docker")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
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
	if rootModeValue == "replicated" && strings.TrimSpace(*workdir) == "" {
		return fmt.Errorf("pd replicated root mode requires -workdir")
	}

	lis, err := pdListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("pd listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	cluster := core.NewCluster()
	var (
		store         pdstorage.Store
		workdirPath   string
		loadedRegions int
	)
	if strings.TrimSpace(*workdir) != "" {
		workdirPath = strings.TrimSpace(*workdir)
		var rootStore *pdstorage.RootStore
		switch rootModeValue {
		case "", "local":
			rootStore, err = pdstorage.OpenRootLocalStore(workdirPath)
		case "replicated":
			clusterIDs, parseErr := parseRootClusterIDs(*rootCluster)
			if parseErr != nil {
				return parseErr
			}
			if len(clusterIDs) != 3 {
				return fmt.Errorf("pd replicated root mode requires exactly 3 root cluster ids")
			}
			rootStore, err = pdstorage.OpenRootReplicatedStore(workdirPath, *rootNodeID, clusterIDs)
		}
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
		_, _ = fmt.Fprintf(w, "PD restored %d region(s) from metadata root: %s\n", loadedRegions, workdirPath)
		_, _ = fmt.Fprintf(w, "PD allocator starts: id=%d ts=%d\n", *idStart, *tsStart)
		_, _ = fmt.Fprintf(w, "PD metadata root mode: %s\n", rootModeValue)
	}
	_, _ = fmt.Fprintf(w, "PD-lite service listening on %s\n", lis.Addr().String())
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "PD metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}
	ctx, cancel := pdNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if refresher, ok := store.(pdstorage.Refresher); ok && rootModeValue == "replicated" {
		ticker := time.NewTicker(*rootRefresh)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					_ = refresher.Refresh()
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

func parseRootClusterIDs(raw string) ([]uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []uint64{1, 2, 3}, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]uint64, 0, len(parts))
	seen := make(map[uint64]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse root cluster id %q: %w", part, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("root cluster ids must be > 0")
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("duplicate root cluster id %d", id)
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("root cluster must contain at least one node id")
	}
	return out, nil
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
