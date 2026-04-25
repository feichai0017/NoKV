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
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultRaftstoreDialTimeout = 3 * time.Second
	defaultMountResolverTTL     = time.Second
)

// RaftstoreOptions configures the default NoKV-backed fsmeta runtime.
type RaftstoreOptions struct {
	CoordinatorAddr  string
	DialOptions      []grpc.DialOption
	DialTimeout      time.Duration
	MountResolverTTL time.Duration
}

// MountRetirer lets operational monitors mark retired mounts in executor-side
// caches without coupling the command package to the resolver implementation.
type MountRetirer interface {
	MarkMountRetired(fsmeta.MountID)
}

// MountLister is the coordinator mount-list surface used by optional lifecycle
// monitors.
type MountLister interface {
	ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
}

// MountRetirementRouter closes live watchers attached to retired mounts.
type MountRetirementRouter interface {
	RetireMount(fsmeta.MountID) int
}

// RaftstoreFsmeta is a complete fsmeta runtime backed by the NoKV raftstore.
// It owns the coordinator, raftstore, and watch-source clients it creates.
type RaftstoreFsmeta struct {
	Executor          *Executor
	Watcher           fsmeta.Watcher
	SnapshotPublisher fsmeta.SnapshotPublisher
	MountResolver     MountResolver
	MountRetirer      MountRetirer
	MountLister       MountLister
	MountRouter       MountRetirementRouter

	close func() error
	once  sync.Once
}

// Close releases watch streams, raftstore connections, and coordinator client
// resources owned by the runtime.
func (r *RaftstoreFsmeta) Close() error {
	if r == nil || r.close == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		err = r.close()
	})
	return err
}

// OpenWithRaftstore builds an fsmeta executor and watcher backed by NoKV's
// raftstore and coordinator services. It is the embedded-user entry point for
// the default production runtime; lower-level NewRaftstoreRunner remains
// available for tests and custom wiring.
func OpenWithRaftstore(ctx context.Context, opts RaftstoreOptions) (*RaftstoreFsmeta, error) {
	if opts.CoordinatorAddr == "" {
		return nil, errors.New("fsmeta/exec: coordinator addr is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dialTimeout := opts.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultRaftstoreDialTimeout
	}
	dialOptions := normalizeDialOptions(opts.DialOptions)
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	coordRPC, err := coordclient.NewGRPCClient(dialCtx, opts.CoordinatorAddr, dialOptions...)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("init coordinator client: %w", err)
	}

	kv, err := client.New(client.Config{
		Context:        ctx,
		StoreResolver:  coordRPC,
		RegionResolver: coordRPC,
		DialOptions:    dialOptions,
	})
	if err != nil {
		_ = coordRPC.Close()
		return nil, fmt.Errorf("init raftstore client: %w", err)
	}
	runner, err := NewRaftstoreRunner(kv, coordRPC)
	if err != nil {
		_ = kv.Close()
		_ = coordRPC.Close()
		return nil, fmt.Errorf("init fsmeta runner: %w", err)
	}

	mountTTL := opts.MountResolverTTL
	if mountTTL == 0 {
		mountTTL = defaultMountResolverTTL
	}
	mountResolver := &coordinatorMountResolver{
		coord: coordRPC,
		ttl:   mountTTL,
	}
	snapshotPublisher := rootSnapshotPublisher{coord: coordRPC}
	executor, err := New(
		runner,
		WithMountResolver(mountResolver),
		WithSubtreeHandoffPublisher(snapshotPublisher),
	)
	if err != nil {
		_ = kv.Close()
		_ = coordRPC.Close()
		return nil, fmt.Errorf("init fsmeta executor: %w", err)
	}

	router := fsmetawatch.NewRouter()
	source, err := fsmetawatch.StartRemoteSource(ctx, coordRPC, router, dialOptions...)
	if err != nil {
		_ = kv.Close()
		_ = coordRPC.Close()
		return nil, fmt.Errorf("init fsmeta watch source: %w", err)
	}
	watcher := fsmetaWatchRuntime{Router: router, source: source, mounts: mountResolver}
	runtime := &RaftstoreFsmeta{
		Executor:          executor,
		Watcher:           watcher,
		SnapshotPublisher: snapshotPublisher,
		MountResolver:     mountResolver,
		MountRetirer:      mountResolver,
		MountLister:       coordRPC,
		MountRouter:       router,
	}
	runtime.close = func() error {
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
	return runtime, nil
}

func normalizeDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	if len(opts) > 0 {
		return append([]grpc.DialOption(nil), opts...)
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}

type fsmetaWatchRuntime struct {
	*fsmetawatch.Router
	source *fsmetawatch.RemoteSource
	mounts MountResolver
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
		return errors.New("root event publisher is not configured")
	}
	resp, err := p.coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return errors.New("root event was not accepted")
	}
	return nil
}

type mountLookupClient interface {
	GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
}

type coordinatorMountResolver struct {
	coord mountLookupClient
	ttl   time.Duration
	now   func() time.Time

	mu      sync.Mutex
	entries map[fsmeta.MountID]mountCacheEntry
}

type mountCacheEntry struct {
	record    MountRecord
	err       error
	expiresAt time.Time
}

func (r *coordinatorMountResolver) ResolveMount(ctx context.Context, mount fsmeta.MountID) (MountRecord, error) {
	if r.coord == nil {
		return MountRecord{}, errors.New("mount resolver is not configured")
	}
	now := r.currentTime()
	if record, err, ok := r.cached(mount, now); ok {
		return record, err
	}
	resp, err := r.coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return MountRecord{}, err
	}
	record, err := mountRecordFromResponse(resp)
	r.store(mount, now, record, err)
	return record, err
}

func (r *coordinatorMountResolver) MarkMountRetired(mount fsmeta.MountID) {
	if mount == "" {
		return
	}
	r.store(mount, r.currentTime(), MountRecord{
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

func (r *coordinatorMountResolver) cached(mount fsmeta.MountID, now time.Time) (MountRecord, error, bool) {
	if r.ttl <= 0 {
		return MountRecord{}, nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[mount]
	if !ok || !now.Before(entry.expiresAt) {
		return MountRecord{}, nil, false
	}
	return entry.record, entry.err, true
}

func (r *coordinatorMountResolver) store(mount fsmeta.MountID, now time.Time, record MountRecord, err error) {
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

func mountRecordFromResponse(resp *coordpb.GetMountResponse) (MountRecord, error) {
	if resp == nil || resp.GetNotFound() {
		return MountRecord{}, fsmeta.ErrMountNotRegistered
	}
	info := resp.GetMount()
	if info == nil {
		return MountRecord{}, fsmeta.ErrMountNotRegistered
	}
	return MountRecord{
		MountID:       fsmeta.MountID(info.GetMountId()),
		RootInode:     fsmeta.InodeID(info.GetRootInode()),
		SchemaVersion: info.GetSchemaVersion(),
		Retired:       info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED,
	}, nil
}
