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
	"path/filepath"
	"strings"
	"syscall"

	"github.com/feichai0017/NoKV/config"
	rootraft "github.com/feichai0017/NoKV/meta/root/raft"
	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"
	"github.com/feichai0017/NoKV/pd/tso"
	"google.golang.org/grpc"
)

var pdNotifyContext = signal.NotifyContext
var pdListen = net.Listen

const (
	pdMetaBackendLocal      = "local"
	pdMetaBackendRaftSingle = "raft-single"
)

func runPDCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("pd", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:2379", "listen address for PD-lite gRPC service")
	idStart := fs.Uint64("id-start", 1, "initial ID allocator value")
	tsStart := fs.Uint64("ts-start", 1, "initial TSO value")
	workdir := fs.String("workdir", "", "optional PD local state directory for persisting allocator and region catalog")
	metaBackend := fs.String("meta-backend", pdMetaBackendLocal, "metadata root backend: local|raft-single")
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
		backendLabel  string
	)
	if strings.TrimSpace(*workdir) != "" {
		workdirPath = strings.TrimSpace(*workdir)
		rootStore, backend, err := openPDRootStore(workdirPath, strings.TrimSpace(*metaBackend))
		if err != nil {
			return fmt.Errorf("pd open metadata root %q (%s): %w", workdirPath, strings.TrimSpace(*metaBackend), err)
		}
		defer func() { _ = rootStore.Close() }()
		bootstrap, err := pdstorage.Bootstrap(rootStore, cluster, *idStart, *tsStart)
		if err != nil {
			return fmt.Errorf("pd bootstrap from %q: %w", workdirPath, err)
		}
		*idStart, *tsStart = bootstrap.IDStart, bootstrap.TSStart
		loadedRegions = bootstrap.LoadedRegions
		store = rootStore
		backendLabel = backend
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
		_, _ = fmt.Fprintf(w, "PD restored %d region(s) from metadata root (%s): %s\n", loadedRegions, backendLabel, workdirPath)
		_, _ = fmt.Fprintf(w, "PD allocator starts: id=%d ts=%d\n", *idStart, *tsStart)
	}
	_, _ = fmt.Fprintf(w, "PD-lite service listening on %s\n", lis.Addr().String())
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "PD metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}
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

func openPDRootStore(workdir, backend string) (pdstorage.Store, string, error) {
	switch normalizePDMetaBackend(backend) {
	case pdMetaBackendLocal:
		store, err := pdstorage.OpenRootLocalStore(workdir)
		return store, pdMetaBackendLocal, err
	case pdMetaBackendRaftSingle:
		root, err := rootraft.OpenSingleNode(rootraft.Config{
			NodeID:  1,
			WorkDir: pdEmbeddedRootRaftDir(workdir),
		})
		if err != nil {
			return nil, "", err
		}
		store, err := pdstorage.OpenRootStore(root)
		if err != nil {
			_ = root.Close()
			return nil, "", err
		}
		return store, pdMetaBackendRaftSingle, nil
	default:
		return nil, "", fmt.Errorf("invalid meta backend %q (expected local|raft-single)", backend)
	}
}

func normalizePDMetaBackend(backend string) string {
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		return pdMetaBackendLocal
	}
	return backend
}

func pdEmbeddedRootRaftDir(workdir string) string {
	return filepath.Join(strings.TrimSpace(workdir), "meta-root-raft")
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
