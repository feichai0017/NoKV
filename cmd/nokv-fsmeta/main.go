// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	fsmetalocal "github.com/feichai0017/NoKV/fsmeta/runtime/local"
	fsmetaraftstore "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	metricspkg "github.com/feichai0017/NoKV/metrics"
	"google.golang.org/grpc"
)

var (
	exit             = os.Exit
	listen           = net.Listen
	signalNotify     = signal.Notify
	openLocalRuntime = fsmetalocal.Open
	openRaftRuntime  = fsmetaraftstore.Open
)

type fsmetaServerRuntime struct {
	executor       fsmetaserver.Executor
	watcher        observe.Watcher
	snapshot       observe.SnapshotPublisher
	close          func() error
	publishStats   func()
	contractLog    string
	startupSummary string
}

type configuredRuntimeOptions struct {
	Kind            string
	Local           fsmetalocal.Options
	CoordinatorAddr string
	Raftstore       fsmetaraftstore.Options
}

func fatalf(format string, args ...any) {
	log.Printf(format, args...)
	exit(1)
}

func main() {
	var (
		addr                = flag.String("addr", "127.0.0.1:8090", "listen address for FSMetadata gRPC server")
		metricsAddr         = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
		localWorkDir        = flag.String("local-work-dir", "nokv-fsmeta-local", "Pebble work directory for the local fsmeta demo runtime")
		localMountID        = flag.String("local-mount-id", "default", "single mount id admitted by the local runtime")
		localMountKeyID     = flag.Uint64("local-mount-key-id", 1, "single mount key id admitted by the local runtime")
		localRootInode      = flag.Uint64("local-root-inode", uint64(model.RootInode), "root inode for the local runtime")
		runtimeKind         = flag.String("runtime", "local", "fsmeta runtime: local or raftstore")
		coordinatorAddr     = flag.String("coordinator-addr", "", "coordinator gRPC address for raftstore runtime")
		bootstrapMount      = flag.String("bootstrap-mount", "", "optional mount id whose root inode should be bootstrapped on raftstore runtime startup")
		_                   = flag.Int("affinity-buckets", layout.DefaultAffinityBucketCount, "accepted for compatibility with older fsmeta demo scripts; local layout owns bucket selection")
		lockTTL             = flag.Duration("lock-ttl", 0, "fsmeta lock TTL; zero uses the fsmeta default")
		sessionCleanupLimit = flag.Uint("session-cleanup-limit", 0, "accepted for compatibility with older fsmeta demo scripts; local runtime has no background session cleaner")
	)
	flag.Parse()
	if *lockTTL < 0 {
		fatalf("lock-ttl must be non-negative")
		return
	}
	if *sessionCleanupLimit > uint(model.MaxSessionExpireLimit) {
		fatalf("session-cleanup-limit exceeds maximum %d", model.MaxSessionExpireLimit)
		return
	}
	localMount, err := localMountIdentity(*localMountID, *localMountKeyID)
	if err != nil {
		fatalf("parse local mount: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := openConfiguredRuntime(ctx, configuredRuntimeOptions{
		Kind: strings.TrimSpace(*runtimeKind),
		Local: fsmetalocal.Options{
			WorkDir:   *localWorkDir,
			Mount:     localMount,
			RootInode: model.InodeID(*localRootInode),
			LockTTL:   *lockTTL,
		},
		CoordinatorAddr: strings.TrimSpace(*coordinatorAddr),
		Raftstore: fsmetaraftstore.Options{
			BootstrapMount: model.MountID(strings.TrimSpace(*bootstrapMount)),
			LockTTL:        *lockTTL,
		},
	})
	if err != nil {
		fatalf("open fsmeta runtime: %v", err)
		return
	}
	defer func() {
		if err := rt.close(); err != nil {
			log.Printf("close fsmeta runtime: %v", err)
		}
	}()

	ln, err := listen("tcp", *addr)
	if err != nil {
		log.Printf("listen: %v", err)
		return
	}
	defer func() { _ = ln.Close() }()

	srv := grpc.NewServer()
	registerFSMetadataServer(srv, rt)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	if *metricsAddr != "" {
		rt.publishStats()
		mln, err := metricspkg.StartExpvarServer(*metricsAddr)
		if err != nil {
			log.Printf("start metrics endpoint: %v", err)
			srv.GracefulStop()
			return
		}
		defer func() {
			if mln != nil {
				_ = mln.Close()
			}
		}()
		log.Printf("expvar metrics listening on http://%s/debug/vars", mln.Addr().String())
	}

	log.Printf("NoKV FSMetadata local server listening on %s", ln.Addr().String())
	log.Print(rt.startupSummary)
	log.Print(rt.contractLog)

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
	srv.GracefulStop()
}

func openConfiguredRuntime(ctx context.Context, opts configuredRuntimeOptions) (*fsmetaServerRuntime, error) {
	switch opts.Kind {
	case "", "local":
		return openConfiguredLocalRuntime(ctx, opts)
	case "raftstore":
		return openConfiguredRaftstoreRuntime(ctx, opts)
	default:
		return nil, fmt.Errorf("unsupported fsmeta runtime %q", opts.Kind)
	}
}

func openConfiguredLocalRuntime(ctx context.Context, opts configuredRuntimeOptions) (*fsmetaServerRuntime, error) {
	rt, err := openLocalRuntime(ctx, opts.Local)
	if err != nil {
		return nil, err
	}
	return localServerRuntime(rt, opts.Local), nil
}

func openConfiguredRaftstoreRuntime(ctx context.Context, opts configuredRuntimeOptions) (*fsmetaServerRuntime, error) {
	if opts.CoordinatorAddr == "" {
		return nil, fmt.Errorf("coordinator-addr is required for raftstore runtime")
	}
	coord, err := coordclient.NewGRPCClient(ctx, opts.CoordinatorAddr)
	if err != nil {
		return nil, err
	}
	raftOpts := opts.Raftstore
	raftOpts.Coordinator = coord
	rt, err := openRaftRuntime(ctx, raftOpts)
	if err != nil {
		_ = coord.Close()
		return nil, err
	}
	return raftstoreServerRuntime(rt, coord, opts), nil
}

func localServerRuntime(rt *fsmetalocal.Runtime, opts fsmetalocal.Options) *fsmetaServerRuntime {
	contractLog := "fsmeta commit contract: local backend uses a Pebble-backed one-node MVCC store; successful metadata writes are durable after the local apply group completes"
	return &fsmetaServerRuntime{
		executor: rt.Executor,
		watcher:  rt.Watcher,
		snapshot: rt.Snapshots,
		close:    rt.Close,
		publishStats: func() {
			publishExpvarOnce("nokv_fsmeta_executor", expvar.Func(func() any { return localExecutorStats(rt) }))
			publishExpvarOnce("nokv_fsmeta_local_runner", expvar.Func(func() any { return rt.Runner.Stats() }))
			publishExpvarOnce("nokv_fsmeta_quota", expvar.Func(func() any { return rt.Quotas.Stats() }))
			publishExpvarOnce("nokv_fsmeta_watch", expvar.Func(func() any { return rt.Watcher.Stats() }))
			publishExpvarOnce("nokv_fsmeta_local_snapshot", expvar.Func(func() any { return rt.Snapshots.Stats() }))
		},
		startupSummary: fmt.Sprintf("fsmeta backend: local pebble work_dir=%q mount=%q mount_key_id=%d", opts.WorkDir, opts.Mount.MountID, opts.Mount.MountKeyID),
		contractLog:    contractLog,
	}
}

func raftstoreServerRuntime(rt *fsmetaraftstore.Runtime, coord *coordclient.GRPCClient, opts configuredRuntimeOptions) *fsmetaServerRuntime {
	contractLog := "fsmeta commit contract: raftstore backend commits MetadataCommand through mount-scoped Rust/OpenRaft apply and Holt atomic metadata batches"
	return &fsmetaServerRuntime{
		executor: rt.Executor,
		watcher:  rt.Watcher,
		snapshot: rt.Snapshot,
		close: func() error {
			var first error
			if err := rt.Close(); err != nil {
				first = err
			}
			if err := coord.Close(); err != nil && first == nil {
				first = err
			}
			return first
		},
		publishStats: func() {
			publishExpvarOnce("nokv_fsmeta_executor", expvar.Func(func() any { return raftstoreExecutorStats(rt) }))
			publishExpvarOnce("nokv_fsmeta_watch", expvar.Func(func() any { return rt.Watcher.Stats() }))
		},
		startupSummary: fmt.Sprintf("fsmeta backend: raftstore coordinator=%q bootstrap_mount=%q", opts.CoordinatorAddr, opts.Raftstore.BootstrapMount),
		contractLog:    contractLog,
	}
}

func raftstoreExecutorStats(rt *fsmetaraftstore.Runtime) map[string]any {
	if rt == nil || rt.Executor == nil {
		return map[string]any{"commit_contract": raftstoreCommitContractStats()}
	}
	stats := rt.Executor.Stats()
	stats["commit_contract"] = raftstoreCommitContractStats()
	return stats
}

func raftstoreCommitContractStats() map[string]any {
	return map[string]any{
		"default_write_path":        "metadata_command_openraft_holt",
		"successful_write_boundary": "durable",
		"durable_boundary":          "mount_raft_commit_plus_holt_atomic_apply",
	}
}

func localExecutorStats(rt *fsmetalocal.Runtime) map[string]any {
	if rt == nil || rt.Executor == nil {
		return map[string]any{"commit_contract": localCommitContractStats()}
	}
	stats := rt.Executor.Stats()
	stats["commit_contract"] = localCommitContractStats()
	return stats
}

func localCommitContractStats() map[string]any {
	return map[string]any{
		"default_write_path":        "local_pebble_mvcc",
		"successful_write_boundary": "durable",
		"durable_boundary":          "pebble_apply_group",
	}
}

func registerFSMetadataServer(reg grpc.ServiceRegistrar, rt *fsmetaServerRuntime) {
	opts := make([]fsmetaserver.Option, 0, 2)
	if rt.watcher != nil {
		opts = append(opts, fsmetaserver.WithWatcher(rt.watcher))
	}
	if rt.snapshot != nil {
		opts = append(opts, fsmetaserver.WithSnapshotPublisher(rt.snapshot))
	}
	fsmetaserver.Register(reg, rt.executor, opts...)
}

func localMountIdentity(mountID string, mountKeyID uint64) (model.MountIdentity, error) {
	id := model.MountID(strings.TrimSpace(mountID))
	if id == "" {
		return model.MountIdentity{}, model.ErrInvalidMountID
	}
	if mountKeyID == 0 {
		return model.MountIdentity{}, model.ErrInvalidMountID
	}
	return model.MountIdentity{MountID: id, MountKeyID: model.MountKeyID(mountKeyID)}, nil
}

func publishExpvarOnce(name string, value expvar.Var) {
	if expvar.Get(name) != nil {
		return
	}
	expvar.Publish(name, value)
}
