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

	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootreplicated "github.com/feichai0017/NoKV/meta/root/backend/replicated"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	"google.golang.org/grpc"
)

var metaRootNotifyContext = signal.NotifyContext
var metaRootListen = net.Listen

func runMetaRootCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("meta-root", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:2380", "listen address for metadata root gRPC service")
	mode := fs.String("mode", "local", "metadata root mode: local|replicated")
	workdir := fs.String("workdir", "", "metadata root work directory")
	nodeID := fs.Uint64("node-id", 1, "metadata root node id for replicated mode")
	transportAddr := fs.String("transport-addr", "", "metadata root raft transport address for replicated mode")
	tickInterval := fs.Duration("tick-interval", 100*time.Millisecond, "replicated root raft tick interval")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	var peerFlags []string
	fs.Func("peer", "replicated metadata root peer mapping in the form nodeID=address (repeatable, exactly 3)", func(value string) error {
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

	modeValue := strings.ToLower(strings.TrimSpace(*mode))
	switch modeValue {
	case "", "local", "replicated":
	default:
		return fmt.Errorf("invalid meta-root mode %q (expected local|replicated)", *mode)
	}

	backend, err := openMetaRootBackend(modeValue, strings.TrimSpace(*workdir), *nodeID, strings.TrimSpace(*transportAddr), *tickInterval, peerFlags)
	if err != nil {
		return err
	}
	if closer, ok := backend.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}
	installMetaRootExpvar(modeValue, backend)

	lis, err := metaRootListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("meta-root listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	grpcServer := grpc.NewServer()
	rootremote.Register(grpcServer, backend)

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
	_, _ = fmt.Fprintf(w, "Metadata root mode: %s\n", modeValue)
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

func openMetaRootBackend(modeValue, workdir string, nodeID uint64, transportAddr string, tickInterval time.Duration, peerFlags []string) (rootremote.Backend, error) {
	switch modeValue {
	case "", "local":
		return rootlocal.Open(workdir, nil)
	case "replicated":
		peers, err := parseReplicatedRootPeers(peerFlags)
		if err != nil {
			return nil, err
		}
		cfg := replicatedMetaRootConfig{
			workdir:       workdir,
			nodeID:        nodeID,
			transportAddr: transportAddr,
			peerAddrs:     peers,
			tickInterval:  tickInterval,
		}
		return openReplicatedMetaRoot(cfg)
	default:
		return nil, fmt.Errorf("invalid meta-root mode %q (expected local|replicated)", modeValue)
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
	if strings.TrimSpace(cfg.workdir) == "" {
		return nil, fmt.Errorf("meta-root: replicated mode requires workdir")
	}
	if cfg.nodeID == 0 {
		return nil, fmt.Errorf("meta-root: replicated mode requires node id > 0")
	}
	if strings.TrimSpace(cfg.transportAddr) == "" {
		return nil, fmt.Errorf("meta-root: replicated mode requires transport address")
	}
	if len(cfg.peerAddrs) != 3 {
		return nil, fmt.Errorf("meta-root: replicated mode requires exactly 3 peer addresses")
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
