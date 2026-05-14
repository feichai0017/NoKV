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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaraftstore "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	metricspkg "github.com/feichai0017/NoKV/metrics"
	"google.golang.org/grpc"
)

var (
	exit         = os.Exit
	listen       = net.Listen
	signalNotify = signal.Notify
	openRuntime  = fsmetaraftstore.Open
)

func fatalf(format string, args ...any) {
	log.Printf(format, args...)
	exit(1)
}

func defaultPerasHolderID() string {
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.TrimSpace(hostname)
		if hostname != "" {
			return "fsmeta/" + hostname
		}
	}
	return "fsmeta/default"
}

func main() {
	var (
		addr                            = flag.String("addr", "127.0.0.1:8090", "listen address for FSMetadata gRPC server")
		coordAddr                       = flag.String("coordinator-addr", "", "coordinator gRPC endpoint used for TSO, routing, and store discovery")
		metricsAddr                     = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
		negCacheDir                     = flag.String("negative-cache-dir", "", "optional slab directory for persistent negative dentry cache")
		dirPageDir                      = flag.String("dirpage-cache-dir", "", "optional slab directory for ReadDirPlus page cache")
		affinityBuckets                 = flag.Int("affinity-buckets", fsmeta.DefaultAffinityBucketCount, "fsmeta placement bucket count used to choose Create inode IDs")
		lockTTL                         = flag.Duration("lock-ttl", 0, "Percolator primary-lock TTL for fsmeta mutations; zero uses the fsmeta default")
		sessionCleanupInterval          = flag.Duration("session-cleanup-interval", 30*time.Second, "interval for expired write-session cleanup; choose about half the smallest expected session TTL; negative disables")
		sessionCleanupLimit             = flag.Uint("session-cleanup-limit", 0, "maximum session records scanned per mount per cleanup pass; zero uses fsmeta default")
		perasHolderID                   = flag.String("peras-holder-id", "", "Peras holder id; empty derives a stable local holder id")
		perasAuthorityTTL               = flag.Duration("peras-authority-ttl", 0, "Peras authority grant TTL; zero uses runtime default")
		perasWitnessStores              = flag.String("peras-witness-stores", "", "comma-separated store IDs used as Peras witnesses; empty uses all UP stores")
		perasWitnessQuorum              = flag.Int("peras-witness-quorum", 0, "Peras witness quorum; zero uses majority")
		perasSegmentWitnessRetries      = flag.Int("peras-segment-witness-retries", 3, "Peras segment witness retries for transient authority lag")
		perasSegmentWitnessRetryBackoff = flag.Duration("peras-segment-witness-retry-backoff", 20*time.Millisecond, "Peras segment witness retry backoff")
		perasSegmentBatchSize           = flag.Int("peras-segment-batch-size", 0, "Peras visible operations per segment before background flush; zero uses runtime default")
		perasSegmentMaxReplayMutations  = flag.Int("peras-segment-max-replay-mutations", 0, "Peras replay mutations per installed segment; zero uses runtime default")
		perasSegmentInstallParallelism  = flag.Int("peras-segment-install-parallelism", 0, "Peras segment installs per flush; zero uses GOMAXPROCS")
		perasSegmentFlushEvery          = flag.Duration("peras-segment-flush-every", 0, "Peras opportunistic segment flush interval; zero uses runtime default")
		perasBackgroundFlushTimeout     = flag.Duration("peras-background-flush-timeout", 0, "timeout for opportunistic Peras background segment install; zero uses runtime default")
		perasBackgroundErrorBackoff     = flag.Duration("peras-background-error-backoff", 0, "backoff after failed opportunistic Peras background segment install; zero uses runtime default")
		perasVisibleLogDir              = flag.String("peras-visible-log-dir", "", "optional local WAL directory for holder visible acknowledgements")
		perasVisibleLogPolicy           = flag.String("peras-visible-log-policy", "flushed", "holder visible WAL sync policy: flushed|fsync-batched|fsync|buffered")
	)
	flag.Parse()
	if *lockTTL < 0 {
		fatalf("lock-ttl must be non-negative")
		return
	}
	if *sessionCleanupLimit > uint(fsmeta.MaxSessionExpireLimit) {
		fatalf("session-cleanup-limit exceeds maximum %d", fsmeta.MaxSessionExpireLimit)
		return
	}
	if *perasAuthorityTTL < 0 || *perasSegmentWitnessRetryBackoff < 0 || *perasSegmentWitnessRetries < 0 || *perasWitnessQuorum < 0 ||
		*perasSegmentBatchSize < 0 || *perasSegmentMaxReplayMutations < 0 || *perasSegmentInstallParallelism < 0 || *perasSegmentFlushEvery < 0 ||
		*perasBackgroundFlushTimeout < 0 || *perasBackgroundErrorBackoff < 0 {
		fatalf("peras options must be non-negative")
		return
	}
	perasStoreIDs, err := parseUintList(*perasWitnessStores)
	if err != nil {
		fatalf("parse peras-witness-stores: %v", err)
		return
	}
	visibleLogDurability, err := parsePerasVisibleLogPolicy(*perasVisibleLogPolicy)
	if err != nil {
		fatalf("parse peras-visible-log-policy: %v", err)
		return
	}
	holderID := strings.TrimSpace(*perasHolderID)
	if holderID == "" {
		holderID = defaultPerasHolderID()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt, err := openRuntime(ctx, fsmetaraftstore.Options{
		CoordinatorAddr:                 *coordAddr,
		NegativeCacheDir:                *negCacheDir,
		DirPageCacheDir:                 *dirPageDir,
		AffinityBuckets:                 *affinityBuckets,
		LockTTL:                         *lockTTL,
		SessionCleanupInterval:          *sessionCleanupInterval,
		SessionCleanupLimit:             uint32(*sessionCleanupLimit),
		PerasHolderID:                   holderID,
		PerasAuthorityTTL:               *perasAuthorityTTL,
		PerasWitnessStoreIDs:            perasStoreIDs,
		PerasWitnessQuorum:              *perasWitnessQuorum,
		PerasSegmentWitnessRetries:      *perasSegmentWitnessRetries,
		PerasSegmentWitnessRetryBackoff: *perasSegmentWitnessRetryBackoff,
		PerasSegmentBatchSize:           *perasSegmentBatchSize,
		PerasSegmentMaxReplayMutations:  *perasSegmentMaxReplayMutations,
		PerasSegmentInstallParallelism:  *perasSegmentInstallParallelism,
		PerasSegmentFlushEvery:          *perasSegmentFlushEvery,
		PerasBackgroundFlushTimeout:     *perasBackgroundFlushTimeout,
		PerasBackgroundErrorBackoff:     *perasBackgroundErrorBackoff,
		PerasVisibleLogDir:              *perasVisibleLogDir,
		PerasVisibleLogDurability:       visibleLogDurability,
	})
	if err != nil {
		fatalf("open fsmeta runtime: %v", err)
		return
	}
	defer func() {
		if err := rt.Close(); err != nil {
			log.Printf("close fsmeta runtime: %v", err)
		}
	}()
	// From here on, prefer logging + return so the deferred Close runs and
	// the raftstore + coordinator clients are released.
	ln, err := listen("tcp", *addr)
	if err != nil {
		log.Printf("listen: %v", err)
		return
	}
	defer func() { _ = ln.Close() }()

	srv := grpc.NewServer()
	fsmetaserver.Register(srv, rt.Executor,
		fsmetaserver.WithWatcher(rt.Watcher),
		fsmetaserver.WithSnapshotPublisher(rt.SnapshotPublisher),
	)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	if *metricsAddr != "" {
		publishExpvarOnce("nokv_fsmeta_executor", expvar.Func(func() any { return rt.Executor.Stats() }))
		if stats, ok := rt.Watcher.(interface{ Stats() map[string]any }); ok {
			publishExpvarOnce("nokv_fsmeta_watch", expvar.Func(func() any { return stats.Stats() }))
		}
		if stats, ok := rt.MountResolver.(interface{ Stats() map[string]any }); ok {
			publishExpvarOnce("nokv_fsmeta_mount", expvar.Func(func() any { return stats.Stats() }))
		}
		if stats, ok := rt.QuotaResolver.(interface{ Stats() map[string]any }); ok {
			publishExpvarOnce("nokv_fsmeta_quota", expvar.Func(func() any { return stats.Stats() }))
		}
		if rt.Peras != nil {
			publishExpvarOnce("nokv_fsmeta_peras", expvar.Func(func() any { return rt.Peras.Stats() }))
		}
		if rt.SessionCleaner != nil {
			publishExpvarOnce("nokv_fsmeta_sessions", expvar.Func(func() any { return rt.SessionCleaner.Stats() }))
		}
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

	log.Printf("NoKV FSMetadata server listening on %s", ln.Addr().String())
	log.Printf("fsmeta commit contract: Peras is the default write path; successful metadata writes are visible immediately, while durable completion is reached by witness/segment install and explicit FlushDurable/FlushTo in embedded APIs")

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

func publishExpvarOnce(name string, value expvar.Var) {
	if expvar.Get(name) != nil {
		return
	}
	expvar.Publish(name, value)
}

func parseUintList(value string) ([]uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, err
		}
		if id != 0 {
			out = append(out, id)
		}
	}
	return out, nil
}

func parsePerasVisibleLogPolicy(value string) (wal.DurabilityPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "flushed":
		return wal.DurabilityFlushed, nil
	case "fsync-batched", "fsync_batched", "batched":
		return wal.DurabilityFsyncBatched, nil
	case "fsync":
		return wal.DurabilityFsync, nil
	case "buffered":
		return wal.DurabilityBuffered, nil
	default:
		return 0, fmt.Errorf("invalid peras visible WAL sync policy %q", value)
	}
}
