package main

import (
	"context"
	"expvar"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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

func main() {
	var (
		addr                   = flag.String("addr", "127.0.0.1:8090", "listen address for FSMetadata gRPC server")
		coordAddr              = flag.String("coordinator-addr", "", "coordinator gRPC endpoint used for TSO, routing, and store discovery")
		metricsAddr            = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
		negCacheDir            = flag.String("negative-cache-dir", "", "optional slab directory for persistent negative dentry cache")
		dirPageDir             = flag.String("dirpage-cache-dir", "", "optional slab directory for ReadDirPlus page cache")
		affinityBuckets        = flag.Int("affinity-buckets", fsmeta.DefaultAffinityBucketCount, "fsmeta placement bucket count used to choose Create inode IDs")
		lockTTL                = flag.Duration("lock-ttl", 0, "Percolator primary-lock TTL for fsmeta mutations; zero uses the fsmeta default")
		sessionCleanupInterval = flag.Duration("session-cleanup-interval", 30*time.Second, "interval for expired write-session cleanup; choose about half the smallest expected session TTL; negative disables")
		sessionCleanupLimit    = flag.Uint("session-cleanup-limit", 0, "maximum session records scanned per mount per cleanup pass; zero uses fsmeta default")
		capsuleHolderID        = flag.String("capsule-holder-id", "", "experimental Capsule holder id; empty disables authority acquisition")
		capsuleFastPath        = flag.Bool("capsule-fast-path", false, "enable experimental Capsule fast commit path against raftstore witnesses")
		capsuleAuthorityTTL    = flag.Duration("capsule-authority-ttl", 0, "Capsule authority grant TTL; zero uses runtime default")
		capsuleWitnessStores   = flag.String("capsule-witness-stores", "", "comma-separated store IDs used as Capsule witnesses; empty uses all UP stores")
		capsuleWitnessQuorum   = flag.Int("capsule-witness-quorum", 0, "Capsule witness quorum; zero uses majority")
		capsuleSubmitRetries   = flag.Int("capsule-submit-retries", 3, "Capsule submit retries for transient witness authority lag")
		capsuleRetryBackoff    = flag.Duration("capsule-submit-retry-backoff", 20*time.Millisecond, "Capsule submit retry backoff")
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
	if *capsuleAuthorityTTL < 0 || *capsuleRetryBackoff < 0 || *capsuleSubmitRetries < 0 || *capsuleWitnessQuorum < 0 {
		fatalf("capsule options must be non-negative")
		return
	}
	capsuleStoreIDs, err := parseUintList(*capsuleWitnessStores)
	if err != nil {
		fatalf("parse capsule-witness-stores: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt, err := openRuntime(ctx, fsmetaraftstore.Options{
		CoordinatorAddr:           *coordAddr,
		NegativeCacheDir:          *negCacheDir,
		DirPageCacheDir:           *dirPageDir,
		AffinityBuckets:           *affinityBuckets,
		LockTTL:                   *lockTTL,
		SessionCleanupInterval:    *sessionCleanupInterval,
		SessionCleanupLimit:       uint32(*sessionCleanupLimit),
		CapsuleHolderID:           *capsuleHolderID,
		CapsuleAuthorityTTL:       *capsuleAuthorityTTL,
		CapsuleFastPath:           *capsuleFastPath,
		CapsuleWitnessStoreIDs:    capsuleStoreIDs,
		CapsuleWitnessQuorum:      *capsuleWitnessQuorum,
		CapsuleSubmitRetries:      *capsuleSubmitRetries,
		CapsuleSubmitRetryBackoff: *capsuleRetryBackoff,
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
