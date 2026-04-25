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
	"sync"
	"syscall"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metricspkg "github.com/feichai0017/NoKV/metrics"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	if coordAddr == "" {
		return nil, nil, nil, nil, fmt.Errorf("coordinator-addr is required")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	coordRPC, err := coordclient.NewGRPCClient(dialCtx, coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	cancel()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("init coordinator client: %w", err)
	}
	kv, err := client.New(client.Config{
		Context:        ctx,
		StoreResolver:  coordRPC,
		RegionResolver: coordRPC,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		_ = coordRPC.Close()
		return nil, nil, nil, nil, fmt.Errorf("init raftstore client: %w", err)
	}
	runner, err := fsmetaexec.NewRaftstoreRunner(kv, coordRPC)
	if err != nil {
		_ = kv.Close()
		_ = coordRPC.Close()
		return nil, nil, nil, nil, fmt.Errorf("init fsmeta runner: %w", err)
	}
	mountResolver := &coordinatorMountResolver{
		coord: coordRPC,
		ttl:   defaultMountResolverTTL,
	}
	snapshotPublisher := rootSnapshotPublisher{coord: coordRPC}
	executor, err := fsmetaexec.New(
		runner,
		fsmetaexec.WithMountResolver(mountResolver),
		fsmetaexec.WithSubtreeHandoffPublisher(snapshotPublisher),
	)
	if err != nil {
		_ = kv.Close()
		_ = coordRPC.Close()
		return nil, nil, nil, nil, fmt.Errorf("init fsmeta executor: %w", err)
	}
	router := fsmetawatch.NewRouter()
	source, err := fsmetawatch.StartRemoteSource(ctx, coordRPC, router, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = kv.Close()
		_ = coordRPC.Close()
		return nil, nil, nil, nil, fmt.Errorf("init fsmeta watch source: %w", err)
	}
	mountMonitor := startMountLifecycleMonitor(ctx, coordRPC, router, mountResolver, defaultMountMonitorInterval)
	// Compose closer: shutdown order is kv first (it holds dialed store conns
	// keyed by coordinator-resolved addresses) then coordRPC. Both errors are
	// reported; the first non-nil wins.
	closer := func() error {
		var errAll error
		if mountMonitor != nil {
			if cerr := mountMonitor.Close(); cerr != nil {
				errAll = cerr
			}
		}
		if cerr := source.Close(); cerr != nil {
			errAll = cerr
		}
		if cerr := kv.Close(); cerr != nil && errAll == nil {
			errAll = cerr
		}
		if cerr := coordRPC.Close(); cerr != nil && errAll == nil {
			errAll = cerr
		}
		return errAll
	}
	return executor, fsmetaWatchRuntime{Router: router, source: source, mounts: mountResolver}, snapshotPublisher, closer, nil
}

type fsmetaWatchRuntime struct {
	*fsmetawatch.Router
	source *fsmetawatch.RemoteSource
	mounts fsmetaexec.MountResolver
}

func (w fsmetaWatchRuntime) Subscribe(ctx context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	if req.Mount != "" && w.mounts != nil {
		record, err := w.mounts.ResolveMount(ctx, req.Mount)
		if err != nil {
			return nil, err
		}
		if record.MountID == "" {
			return nil, fsmeta.ErrMountNotRegistered
		}
		if record.Retired {
			return nil, fsmeta.ErrMountRetired
		}
	}
	if w.Router == nil {
		return nil, fsmeta.ErrInvalidRequest
	}
	return w.Router.Subscribe(ctx, req)
}

func (w fsmetaWatchRuntime) Stats() map[string]any {
	out := map[string]any{}
	if w.Router != nil {
		for key, value := range w.Router.Stats() {
			out[key] = value
		}
	}
	if w.source != nil {
		for key, value := range w.source.Stats() {
			out[key] = value
		}
	}
	return out
}

type rootSnapshotPublisher struct {
	coord *coordclient.GRPCClient
}

type mountLookupClient interface {
	GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
}

type mountListClient interface {
	ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
}

type mountRetirementRouter interface {
	RetireMount(fsmeta.MountID) int
}

type coordinatorMountResolver struct {
	coord mountLookupClient
	ttl   time.Duration
	now   func() time.Time

	mu      sync.Mutex
	entries map[fsmeta.MountID]mountCacheEntry
}

type mountCacheEntry struct {
	record    fsmetaexec.MountRecord
	err       error
	expiresAt time.Time
}

func (r *coordinatorMountResolver) ResolveMount(ctx context.Context, mount fsmeta.MountID) (fsmetaexec.MountRecord, error) {
	if r.coord == nil {
		return fsmetaexec.MountRecord{}, fmt.Errorf("mount resolver is not configured")
	}
	now := r.currentTime()
	if record, err, ok := r.cached(mount, now); ok {
		return record, err
	}
	resp, err := r.coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return fsmetaexec.MountRecord{}, err
	}
	record, err := mountRecordFromResponse(resp)
	r.store(mount, now, record, err)
	return record, err
}

func (r *coordinatorMountResolver) MarkMountRetired(mount fsmeta.MountID) {
	if mount == "" {
		return
	}
	r.store(mount, r.currentTime(), fsmetaexec.MountRecord{
		MountID: mount,
		Retired: true,
	}, nil)
}

func (r *coordinatorMountResolver) currentTime() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *coordinatorMountResolver) cached(mount fsmeta.MountID, now time.Time) (fsmetaexec.MountRecord, error, bool) {
	if r.ttl <= 0 {
		return fsmetaexec.MountRecord{}, nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[mount]
	if !ok || !now.Before(entry.expiresAt) {
		return fsmetaexec.MountRecord{}, nil, false
	}
	return entry.record, entry.err, true
}

func (r *coordinatorMountResolver) store(mount fsmeta.MountID, now time.Time, record fsmetaexec.MountRecord, err error) {
	if r.ttl <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[fsmeta.MountID]mountCacheEntry)
	}
	r.entries[mount] = mountCacheEntry{
		record:    record,
		err:       err,
		expiresAt: now.Add(r.ttl),
	}
}

func mountRecordFromResponse(resp *coordpb.GetMountResponse) (fsmetaexec.MountRecord, error) {
	if resp == nil || resp.GetNotFound() {
		return fsmetaexec.MountRecord{}, fsmeta.ErrMountNotRegistered
	}
	info := resp.GetMount()
	if info == nil {
		return fsmetaexec.MountRecord{}, fsmeta.ErrMountNotRegistered
	}
	return fsmetaexec.MountRecord{
		MountID:       fsmeta.MountID(info.GetMountId()),
		RootInode:     fsmeta.InodeID(info.GetRootInode()),
		SchemaVersion: info.GetSchemaVersion(),
		Retired:       info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED,
	}, nil
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

func (p rootSnapshotPublisher) PublishSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error {
	return p.publish(ctx, rootevent.SnapshotEpochPublished(string(token.Mount), uint64(token.RootInode), token.ReadVersion))
}

func (p rootSnapshotPublisher) RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error {
	return p.publish(ctx, rootevent.SnapshotEpochRetired(string(token.Mount), uint64(token.RootInode), token.ReadVersion))
}

func (p rootSnapshotPublisher) StartSubtreeHandoff(ctx context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	return p.publish(ctx, rootevent.SubtreeHandoffStarted(string(mount), uint64(root), frontier))
}

func (p rootSnapshotPublisher) CompleteSubtreeHandoff(ctx context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	return p.publish(ctx, rootevent.SubtreeHandoffCompleted(string(mount), uint64(root), frontier))
}

func (p rootSnapshotPublisher) publish(ctx context.Context, event rootevent.Event) error {
	if p.coord == nil {
		return fmt.Errorf("root event publisher is not configured")
	}
	resp, err := p.coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return fmt.Errorf("root event was not accepted")
	}
	return nil
}
