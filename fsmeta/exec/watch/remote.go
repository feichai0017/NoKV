package watch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultStoreRefreshInterval = time.Second

// StoreLister returns the runtime store registry snapshot.
type StoreLister interface {
	ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error)
}

// RemoteSource subscribes to apply-watch streams from UP stores and feeds their
// events into one fsmeta watch router. v0 is live-only: reconnects resume from
// new events only, not from the last delivered cursor.
type RemoteSource struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	lister StoreLister
	router *Router
	opts   []grpc.DialOption

	mu     sync.Mutex
	stores map[uint64]*remoteStoreWatch
}

type remoteStoreWatch struct {
	addr   string
	cancel context.CancelFunc
	conn   *grpc.ClientConn
}

// StartRemoteSource starts apply-watch streams for all UP stores in lister.
func StartRemoteSource(ctx context.Context, lister StoreLister, router *Router, opts ...grpc.DialOption) (*RemoteSource, error) {
	if lister == nil {
		return nil, fmt.Errorf("fsmeta/watch: store lister is required")
	}
	if router == nil {
		return nil, fmt.Errorf("fsmeta/watch: router is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	streamCtx, cancel := context.WithCancel(ctx)
	source := &RemoteSource{
		ctx:    streamCtx,
		cancel: cancel,
		lister: lister,
		router: router,
		opts:   normalizeDialOptions(opts),
		stores: make(map[uint64]*remoteStoreWatch),
	}
	if err := source.syncStores(streamCtx); err != nil {
		_ = source.Close()
		return nil, err
	}
	source.wg.Add(1)
	go source.runDiscovery()
	return source, nil
}

func (s *RemoteSource) runDiscovery() {
	defer s.wg.Done()
	ticker := time.NewTicker(defaultStoreRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			_ = s.syncStores(s.ctx)
		}
	}
}

func (s *RemoteSource) syncStores(ctx context.Context) error {
	resp, err := s.lister.ListStores(ctx, &coordpb.ListStoresRequest{})
	if err != nil {
		return err
	}
	active := make(map[uint64]*coordpb.StoreInfo)
	for _, store := range resp.GetStores() {
		if store.GetState() == coordpb.StoreState_STORE_STATE_UP && store.GetClientAddr() != "" {
			active[store.GetStoreId()] = store
		}
	}

	var stop []*remoteStoreWatch
	s.mu.Lock()
	for id, watch := range s.stores {
		store, ok := active[id]
		if !ok || store.GetClientAddr() != watch.addr {
			stop = append(stop, watch)
			delete(s.stores, id)
		}
	}
	s.mu.Unlock()
	for _, watch := range stop {
		_ = stopRemoteStore(watch)
	}
	for id, store := range active {
		if err := s.ensureStore(ctx, id, store.GetClientAddr()); err != nil {
			return err
		}
	}
	return nil
}

func (s *RemoteSource) ensureStore(ctx context.Context, storeID uint64, addr string) error {
	s.mu.Lock()
	if existing := s.stores[storeID]; existing != nil && existing.addr == addr {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	conn, err := grpc.NewClient(addr, s.opts...)
	if err != nil {
		return fmt.Errorf("fsmeta/watch: dial store %d: %w", storeID, err)
	}
	storeCtx, cancel := context.WithCancel(ctx)
	watch := &remoteStoreWatch{addr: addr, cancel: cancel, conn: conn}

	s.mu.Lock()
	if existing := s.stores[storeID]; existing != nil && existing.addr == addr {
		s.mu.Unlock()
		_ = stopRemoteStore(watch)
		return nil
	}
	var old *remoteStoreWatch
	if existing := s.stores[storeID]; existing != nil {
		old = existing
	}
	s.stores[storeID] = watch
	s.mu.Unlock()
	_ = stopRemoteStore(old)

	s.wg.Add(1)
	go s.runStore(storeCtx, storeID, conn)
	return nil
}

func (s *RemoteSource) runStore(ctx context.Context, storeID uint64, conn *grpc.ClientConn) {
	defer s.wg.Done()
	defer s.removeStore(storeID, conn)
	stream, err := kvrpcpb.NewNoKVClient(conn).KvWatchApply(ctx, &kvrpcpb.ApplyWatchRequest{})
	if err != nil {
		return
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return
		}
		publishApplyWatchEvent(s.router, resp.GetEvent())
	}
}

func (s *RemoteSource) removeStore(storeID uint64, conn *grpc.ClientConn) {
	s.mu.Lock()
	watch := s.stores[storeID]
	if watch == nil || watch.conn != conn {
		s.mu.Unlock()
		return
	}
	delete(s.stores, storeID)
	s.mu.Unlock()
	_ = stopRemoteStore(watch)
}

// Close stops all remote apply-watch streams and closes store connections.
func (s *RemoteSource) Close() error {
	if s == nil {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	stores := make([]*remoteStoreWatch, 0, len(s.stores))
	for id, watch := range s.stores {
		delete(s.stores, id)
		stores = append(stores, watch)
	}
	s.mu.Unlock()
	var first error
	for _, watch := range stores {
		if err := stopRemoteStore(watch); err != nil && first == nil {
			first = err
		}
	}
	s.wg.Wait()
	return first
}

func stopRemoteStore(watch *remoteStoreWatch) error {
	if watch == nil {
		return nil
	}
	if watch.cancel != nil {
		watch.cancel()
	}
	if watch.conn != nil {
		return watch.conn.Close()
	}
	return nil
}

func publishApplyWatchEvent(router *Router, evt *kvrpcpb.ApplyWatchEvent) {
	if router == nil || evt == nil {
		return
	}
	source := applyWatchSourceFromProto(evt.GetSource())
	if source == 0 {
		return
	}
	for _, key := range evt.GetKeys() {
		router.Publish(newWatchEvent(evt, source, key))
	}
}

func newWatchEvent(evt *kvrpcpb.ApplyWatchEvent, source fsmeta.WatchEventSource, key []byte) fsmeta.WatchEvent {
	return fsmeta.WatchEvent{
		Cursor: fsmeta.WatchCursor{
			RegionID: evt.GetRegionId(),
			Term:     evt.GetTerm(),
			Index:    evt.GetIndex(),
		},
		CommitVersion: evt.GetCommitVersion(),
		Source:        source,
		Key:           append([]byte(nil), key...),
	}
}

func applyWatchSourceFromProto(source kvrpcpb.ApplyWatchEventSource) fsmeta.WatchEventSource {
	switch source {
	case kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_COMMIT:
		return fsmeta.WatchEventSourceCommit
	case kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_RESOLVE_LOCK:
		return fsmeta.WatchEventSourceResolveLock
	default:
		return 0
	}
}

func normalizeDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	if len(opts) > 0 {
		return append([]grpc.DialOption(nil), opts...)
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}
