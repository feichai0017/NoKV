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
	executor, err := fsmetaexec.New(runner, fsmetaexec.WithMountResolver(coordinatorMountResolver{coord: coordRPC}))
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
	snapshotPublisher := rootSnapshotPublisher{coord: coordRPC}
	// Compose closer: shutdown order is kv first (it holds dialed store conns
	// keyed by coordinator-resolved addresses) then coordRPC. Both errors are
	// reported; the first non-nil wins.
	closer := func() error {
		var errAll error
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
	return executor, fsmetaWatchRuntime{Router: router, source: source}, snapshotPublisher, closer, nil
}

type fsmetaWatchRuntime struct {
	*fsmetawatch.Router
	source *fsmetawatch.RemoteSource
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

type coordinatorMountResolver struct {
	coord *coordclient.GRPCClient
}

func (r coordinatorMountResolver) ResolveMount(ctx context.Context, mount fsmeta.MountID) (fsmetaexec.MountRecord, error) {
	if r.coord == nil {
		return fsmetaexec.MountRecord{}, fmt.Errorf("mount resolver is not configured")
	}
	resp, err := r.coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return fsmetaexec.MountRecord{}, err
	}
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

func (p rootSnapshotPublisher) PublishSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error {
	return p.publish(ctx, rootevent.SnapshotEpochPublished(string(token.Mount), uint64(token.RootInode), token.ReadVersion))
}

func (p rootSnapshotPublisher) RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error {
	return p.publish(ctx, rootevent.SnapshotEpochRetired(string(token.Mount), uint64(token.RootInode), token.ReadVersion))
}

func (p rootSnapshotPublisher) publish(ctx context.Context, event rootevent.Event) error {
	if p.coord == nil {
		return fmt.Errorf("snapshot subtree publisher is not configured")
	}
	resp, err := p.coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return fmt.Errorf("snapshot subtree root event was not accepted")
	}
	return nil
}
