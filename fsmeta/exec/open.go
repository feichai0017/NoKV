package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultDialTimeout = 3 * time.Second

// Options configures the default NoKV-backed fsmeta runtime.
type Options struct {
	CoordinatorAddr string
	DialOptions     []grpc.DialOption
	DialTimeout     time.Duration
	MountTTL        time.Duration
	QuotaTTL        time.Duration

	// MonitorInterval controls rooted lifecycle stream reconnect backoff.
	// Zero uses the package default; negative disables the monitor.
	MonitorInterval time.Duration
}

// Runtime is a complete fsmeta runtime backed by the NoKV raftstore. It owns
// every client and goroutine it creates; Close releases all of them.
type Runtime struct {
	Executor          *Executor
	Watcher           fsmeta.Watcher
	SnapshotPublisher fsmeta.SnapshotPublisher
	MountResolver     MountResolver
	QuotaResolver     QuotaResolver

	close func() error
	once  sync.Once
}

// Close releases watch streams, raftstore connections, the mount monitor, and
// coordinator client resources owned by the runtime.
func (r *Runtime) Close() error {
	if r == nil || r.close == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		err = r.close()
	})
	return err
}

// OpenWithRaftstore builds an fsmeta runtime backed by NoKV's raftstore and
// coordinator. It is the embedded-user entry point; lower-level
// NewRaftstoreRunner remains available for tests and custom wiring.
func OpenWithRaftstore(ctx context.Context, opts Options) (*Runtime, error) {
	if opts.CoordinatorAddr == "" {
		return nil, errors.New("fsmeta/exec: coordinator addr is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dialTimeout := opts.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}
	dialOpts := dialOptions(opts.DialOptions)
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	coord, err := coordclient.NewGRPCClient(dialCtx, opts.CoordinatorAddr, dialOpts...)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("dial coordinator: %w", err)
	}

	kv, err := client.New(client.Config{
		Context:        ctx,
		StoreResolver:  coord,
		RegionResolver: coord,
		DialOptions:    dialOpts,
	})
	if err != nil {
		_ = coord.Close()
		return nil, fmt.Errorf("dial raftstore: %w", err)
	}
	runner, err := NewRaftstoreRunner(kv, coord)
	if err != nil {
		_ = kv.Close()
		_ = coord.Close()
		return nil, fmt.Errorf("init runner: %w", err)
	}

	mountTTL := opts.MountTTL
	if mountTTL == 0 {
		mountTTL = defaultMountTTL
	}
	mounts := &mountCache{coord: coord, ttl: mountTTL}
	quotaTTL := opts.QuotaTTL
	if quotaTTL == 0 {
		quotaTTL = defaultQuotaTTL
	}
	quotas := &quotaCache{coord: coord, ttl: quotaTTL}
	pub := rootPublisher{coord: coord}
	exec, err := New(runner, WithMountResolver(mounts), WithQuotaResolver(quotas), WithSubtreeHandoffPublisher(pub))
	if err != nil {
		_ = kv.Close()
		_ = coord.Close()
		return nil, fmt.Errorf("init executor: %w", err)
	}

	router := fsmetawatch.NewRouter()
	source, err := fsmetawatch.StartRemoteSource(ctx, coord, router, dialOpts...)
	if err != nil {
		_ = kv.Close()
		_ = coord.Close()
		return nil, fmt.Errorf("init watch source: %w", err)
	}

	var mon *monitor
	if opts.MonitorInterval >= 0 {
		mon = startMonitor(ctx, coord, router, mounts, quotas, pub, opts.MonitorInterval)
	}

	rt := &Runtime{
		Executor:          exec,
		Watcher:           watcher{Router: router, source: source, mounts: mounts},
		SnapshotPublisher: pub,
		MountResolver:     mounts,
		QuotaResolver:     quotas,
	}
	rt.close = func() error {
		var first error
		if mon != nil {
			if err := mon.Close(); err != nil {
				first = err
			}
		}
		if err := source.Close(); err != nil && first == nil {
			first = err
		}
		if err := kv.Close(); err != nil && first == nil {
			first = err
		}
		if err := coord.Close(); err != nil && first == nil {
			first = err
		}
		return first
	}
	return rt, nil
}

func dialOptions(opts []grpc.DialOption) []grpc.DialOption {
	if len(opts) > 0 {
		return append([]grpc.DialOption(nil), opts...)
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}
