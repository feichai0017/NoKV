package main

import (
	"context"
	"expvar"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	metricspkg "github.com/feichai0017/NoKV/metrics"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
)

var exit = os.Exit

var fatalf = func(format string, args ...any) {
	log.Printf(format, args...)
	exit(1)
}

var listen = net.Listen
var signalNotify = signal.Notify
var openExecutor = newExecutor

const (
	defaultMountResolverTTL     = time.Second
	defaultMountMonitorInterval = time.Second
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:8090", "listen address for FSMetadata gRPC server")
		coordAddr   = flag.String("coordinator-addr", "", "coordinator gRPC endpoint used for TSO, routing, and store discovery")
		metricsAddr = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor, watcher, snapshotPublisher, closeExecutor, err := openExecutor(ctx, *coordAddr)
	if err != nil {
		fatalf("open fsmeta executor: %v", err)
		return
	}
	defer func() {
		if closeExecutor != nil {
			if err := closeExecutor(); err != nil {
				log.Printf("close fsmeta executor: %v", err)
			}
		}
	}()
	// From this point on, prefer logging + return so the deferred closer above
	// runs and the raftstore + coordinator clients are released. Calling
	// fatalf directly would invoke os.Exit and skip the defers, leaking
	// connections.
	ln, err := listen("tcp", *addr)
	if err != nil {
		log.Printf("listen: %v", err)
		return
	}
	defer func() { _ = ln.Close() }()

	grpcServer := grpc.NewServer()
	fsmetaserver.Register(grpcServer, executor, fsmetaserver.WithWatcher(watcher), fsmetaserver.WithSnapshotPublisher(snapshotPublisher))
	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(ln)
	}()

	if *metricsAddr != "" {
		publishExpvarOnce("nokv_fsmeta_executor", expvar.Func(func() any { return executor.Stats() }))
		if stats, ok := watcher.(interface{ Stats() map[string]any }); ok {
			publishExpvarOnce("nokv_fsmeta_watch", expvar.Func(func() any { return stats.Stats() }))
		}
		metricsLn, err := metricspkg.StartExpvarServer(*metricsAddr)
		if err != nil {
			log.Printf("start metrics endpoint: %v", err)
			grpcServer.GracefulStop()
			return
		}
		defer func() {
			if metricsLn != nil {
				_ = metricsLn.Close()
			}
		}()
		log.Printf("expvar metrics listening on http://%s/debug/vars", metricsLn.Addr().String())
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
	grpcServer.GracefulStop()
}

func publishExpvarOnce(name string, value expvar.Var) {
	if expvar.Get(name) != nil {
		return
	}
	expvar.Publish(name, value)
}

type closeFunc func() error

func newExecutor(ctx context.Context, coordAddr string) (*fsmetaexec.Executor, fsmeta.Watcher, fsmeta.SnapshotPublisher, closeFunc, error) {
	runtime, err := fsmetaexec.OpenWithRaftstore(ctx, fsmetaexec.RaftstoreOptions{
		CoordinatorAddr:  coordAddr,
		MountResolverTTL: defaultMountResolverTTL,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	mountMonitor := startMountLifecycleMonitor(ctx, runtime.MountLister, runtime.MountRouter, runtime.MountRetirer, defaultMountMonitorInterval)
	closer := func() error {
		var errAll error
		if mountMonitor != nil {
			if cerr := mountMonitor.Close(); cerr != nil {
				errAll = cerr
			}
		}
		if cerr := runtime.Close(); cerr != nil && errAll == nil {
			errAll = cerr
		}
		return errAll
	}
	return runtime.Executor, runtime.Watcher, runtime.SnapshotPublisher, closer, nil
}

type mountListClient interface {
	ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
}

type mountRetirementRouter interface {
	RetireMount(fsmeta.MountID) int
}

type mountLifecycleMonitor struct {
	coord    mountListClient
	router   mountRetirementRouter
	resolver interface {
		MarkMountRetired(fsmeta.MountID)
	}
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func startMountLifecycleMonitor(ctx context.Context, coord mountListClient, router mountRetirementRouter, resolver interface {
	MarkMountRetired(fsmeta.MountID)
}, interval time.Duration) *mountLifecycleMonitor {
	if coord == nil || router == nil {
		return nil
	}
	if interval <= 0 {
		interval = defaultMountMonitorInterval
	}
	monitor := &mountLifecycleMonitor{
		coord:    coord,
		router:   router,
		resolver: resolver,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go monitor.run(ctx)
	return monitor
}

func (m *mountLifecycleMonitor) run(ctx context.Context) {
	defer close(m.done)
	_ = m.poll(ctx)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stop:
			return
		case <-ticker.C:
			if err := m.poll(ctx); err != nil && ctx.Err() == nil {
				log.Printf("fsmeta mount lifecycle monitor: %v", err)
			}
		}
	}
}

func (m *mountLifecycleMonitor) poll(ctx context.Context) error {
	if m == nil || m.coord == nil || m.router == nil {
		return nil
	}
	resp, err := m.coord.ListMounts(ctx, &coordpb.ListMountsRequest{})
	if err != nil {
		return err
	}
	for _, mount := range resp.GetMounts() {
		if mount.GetMountId() == "" || mount.GetState() != coordpb.MountState_MOUNT_STATE_RETIRED {
			continue
		}
		mountID := fsmeta.MountID(mount.GetMountId())
		if m.resolver != nil {
			m.resolver.MarkMountRetired(mountID)
		}
		m.router.RetireMount(mountID)
	}
	return nil
}

func (m *mountLifecycleMonitor) Close() error {
	if m == nil {
		return nil
	}
	m.once.Do(func() {
		close(m.stop)
	})
	<-m.done
	return nil
}
