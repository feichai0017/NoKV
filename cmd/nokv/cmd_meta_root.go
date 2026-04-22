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
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/config"
	rootreplicated "github.com/feichai0017/NoKV/meta/root/replicated"
	rootserver "github.com/feichai0017/NoKV/meta/root/server"
	"google.golang.org/grpc"
)

var metaRootNotifyContext = signal.NotifyContext
var metaRootListen = net.Listen

// runMetaRootCmd launches one replicated metadata-root peer. NoKV only ships
// the separated-truth-plane topology: meta-root is always a 3-peer raft
// quorum. Single-process/local mode has been removed from the CLI.
func runMetaRootCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("meta-root", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:2380", "listen address for metadata root gRPC service (resolved from --config if present)")
	workdir := fs.String("workdir", "", "metadata root work directory (resolved from --config if present)")
	nodeID := fs.Uint64("node-id", 0, "metadata root node id (required, must be > 0)")
	transportAddr := fs.String("transport-addr", "", "metadata root raft transport address (resolved from --config if present)")
	tickInterval := fs.Duration("tick-interval", 100*time.Millisecond, "replicated root raft tick interval")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	configPath := fs.String("config", "", "optional raft configuration file (meta_root.peers drives --peer/--transport-addr/--workdir)")
	scope := fs.String("scope", "host", "scope for config-resolved addresses: host|docker")
	var peerFlags []string
	fs.Func("peer", "metadata root peer mapping in the form nodeID=address (repeatable, exactly 3; resolved from --config if present)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("peer value cannot be empty")
		}
		peerFlags = append(peerFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Resolve --peer / --transport-addr / --addr / --workdir from the config
	// file when present. Explicit CLI flags always win.
	if strings.TrimSpace(*configPath) != "" {
		scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
		if scopeNorm != "host" && scopeNorm != "docker" {
			return fmt.Errorf("invalid meta-root scope %q (expected host|docker)", *scope)
		}
		cfg, err := config.LoadFile(strings.TrimSpace(*configPath))
		if err != nil {
			return fmt.Errorf("meta-root load config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("meta-root validate config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if cfg.MetaRoot != nil {
			if !flagPassed(fs, "peer") {
				for id, paddr := range cfg.MetaRootTransportPeers(scopeNorm) {
					peerFlags = append(peerFlags, fmt.Sprintf("%d=%s", id, paddr))
				}
			}
			if *nodeID != 0 {
				if !flagPassed(fs, "addr") {
					if resolved := cfg.ResolveMetaRootServiceAddr(*nodeID, scopeNorm); resolved != "" {
						*addr = resolved
					}
				}
				if !flagPassed(fs, "transport-addr") {
					if resolved := cfg.ResolveMetaRootTransportAddr(*nodeID, scopeNorm); resolved != "" {
						*transportAddr = resolved
					}
				}
				if !flagPassed(fs, "workdir") {
					if resolved := cfg.ResolveMetaRootWorkDir(*nodeID, scopeNorm); resolved != "" {
						*workdir = resolved
					}
				}
			}
		}
	}

	cfg := replicatedMetaRootConfig{
		workdir:       strings.TrimSpace(*workdir),
		nodeID:        *nodeID,
		transportAddr: strings.TrimSpace(*transportAddr),
		tickInterval:  *tickInterval,
	}
	peers, err := parseReplicatedRootPeers(peerFlags)
	if err != nil {
		return err
	}
	cfg.peerAddrs = peers

	backend, err := openReplicatedMetaRoot(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = backend.Close() }()

	lis, err := metaRootListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("meta-root listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	grpcServer := grpc.NewServer()
	rootserver.Register(grpcServer, backend)

	installMetaRootExpvar(metaRootExpvarContext{
		addr:    lis.Addr().String(),
		nodeID:  *nodeID,
		backend: backend,
	})

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()

	metricsLn, err := startExpvarServer(*metricsAddr)
	if err != nil {
		return fmt.Errorf("start meta-root metrics endpoint: %w", err)
	}
	if metricsLn != nil {
		defer func() { _ = metricsLn.Close() }()
	}

	_, _ = fmt.Fprintf(w, "Metadata root service listening on %s\n", lis.Addr().String())
	_, _ = fmt.Fprintf(w, "Metadata root node id: %d\n", *nodeID)
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "Metadata root metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}

	ctx, cancel := metaRootNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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

type replicatedMetaRootConfig struct {
	workdir       string
	nodeID        uint64
	transportAddr string
	peerAddrs     map[uint64]string
	tickInterval  time.Duration
}

func openReplicatedMetaRoot(cfg replicatedMetaRootConfig) (*rootreplicated.Store, error) {
	if cfg.workdir == "" {
		return nil, fmt.Errorf("meta-root: --workdir is required")
	}
	if cfg.nodeID == 0 {
		return nil, fmt.Errorf("meta-root: --node-id is required and must be > 0")
	}
	if cfg.transportAddr == "" {
		return nil, fmt.Errorf("meta-root: --transport-addr is required")
	}
	if len(cfg.peerAddrs) != 3 {
		return nil, fmt.Errorf("meta-root: requires exactly 3 --peer values")
	}
	transport, err := rootreplicated.NewGRPCTransport(cfg.nodeID, cfg.transportAddr)
	if err != nil {
		return nil, err
	}
	transport.SetPeers(cfg.peerAddrs)
	driver, err := rootreplicated.NewNetworkDriver(rootreplicated.NetworkConfig{
		ID:           cfg.nodeID,
		WorkDir:      cfg.workdir,
		PeerIDs:      replicatedPeerIDs(cfg.peerAddrs),
		Transport:    transport,
		TickInterval: cfg.tickInterval,
	})
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	store, err := rootreplicated.Open(rootreplicated.Config{Driver: driver})
	if err != nil {
		_ = driver.Close()
		return nil, err
	}
	return store, nil
}

func replicatedPeerIDs(peers map[uint64]string) []uint64 {
	ids := make([]uint64, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
