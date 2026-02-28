package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
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
	workdir := fs.String("workdir", "", "optional manifest work directory for persisting/loading PD region catalog")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve pd workdir")
	scope := fs.String("scope", "host", "scope for config-resolved pd workdir: host|docker")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*workdir) == "" && strings.TrimSpace(*configPath) != "" {
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
		if resolved := cfg.ResolvePDWorkDir(scopeNorm); resolved != "" {
			*workdir = resolved
		}
	}

	lis, err := pdListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("pd listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	cluster := core.NewCluster()
	var (
		stateStore    *pdStateStore
		regionCatalog *manifest.Manager
		workdirPath   string
		loadedRegions int
	)
	if strings.TrimSpace(*workdir) != "" {
		workdirPath = strings.TrimSpace(*workdir)
		stateStore = newPDStateStore(workdirPath)
		state, err := stateStore.Load()
		if err != nil {
			return fmt.Errorf("pd load allocator state from %q: %w", workdirPath, err)
		}
		*idStart, *tsStart = resolveAllocatorStarts(*idStart, *tsStart, state)

		mgr, err := manifest.Open(workdirPath, nil)
		if err != nil {
			return fmt.Errorf("pd open manifest workdir %q: %w", workdirPath, err)
		}
		defer func() { _ = mgr.Close() }()
		regionCatalog = mgr

		loadedRegions, err = restorePDRegions(cluster, mgr.RegionSnapshot())
		if err != nil {
			return fmt.Errorf("pd restore regions from %q: %w", workdirPath, err)
		}
	}

	ids := core.NewIDAllocator(*idStart)
	tsAlloc := tso.NewAllocator(*tsStart)
	svc := pdserver.NewService(cluster, ids, tsAlloc)
	if regionCatalog != nil {
		svc.SetRegionCatalog(regionCatalog)
	}
	if stateStore != nil {
		svc.SetAllocatorStateSink(stateStore)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPDServer(grpcServer, svc)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()

	if regionCatalog != nil {
		_, _ = fmt.Fprintf(w, "PD restored %d region(s) from manifest: %s\n", loadedRegions, workdirPath)
		_, _ = fmt.Fprintf(w, "PD allocator starts: id=%d ts=%d\n", *idStart, *tsStart)
	}
	_, _ = fmt.Fprintf(w, "PD-lite service listening on %s\n", lis.Addr().String())
	ctx, cancel := pdNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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

func restorePDRegions(cluster *core.Cluster, snapshot map[uint64]manifest.RegionMeta) (int, error) {
	if cluster == nil || len(snapshot) == 0 {
		return 0, nil
	}
	ids := make([]uint64, 0, len(snapshot))
	for id := range snapshot {
		if id == 0 {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	loaded := 0
	for _, id := range ids {
		meta := snapshot[id]
		if meta.ID == 0 {
			continue
		}
		if err := cluster.UpsertRegionHeartbeat(meta); err != nil {
			return loaded, err
		}
		loaded++
	}
	return loaded, nil
}
