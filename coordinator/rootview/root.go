// Package rootview is the coordinator-side view onto the remote metadata-root
// cluster. It wraps the gRPC remote client with snapshot caching, rooted-tail
// subscriptions, and bootstrap helpers so that coordinator/server can treat
// the 3-peer replicated meta-root as a single source of truth. This package
// never opens a local backend — the only supported topology is remote.
package rootview

import (
	"context"
	"errors"
	"sync"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/meta/topology"
)

var errGrantCommandUnsupported = errors.New("coordinator/rootview: coordinator grant command unsupported")

// RootStorage persists control-plane mutations into durable metadata truth and
// exposes the reconstructed rooted snapshot back to Coordinator.
type RootStorage interface {
	Load() (Snapshot, error)
	AppendRootEvent(ctx context.Context, event rootevent.Event) error
	SaveAllocatorState(ctx context.Context, idCurrent, tsCurrent uint64) error
	ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error)
	Refresh() error
	CanSubmitRootWrites() bool
	LeaderID() uint64
	Close() error
}

type rootBackend interface {
	Snapshot() (rootstate.Snapshot, error)
	Append(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error)
	FenceAllocator(ctx context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error)
}

type rootRuntimeBackend interface {
	rootBackend
	Refresh() error
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	TailNotify() <-chan struct{}
	ObserveCommitted() (rootstorage.ObservedCommitted, error)
	CanSubmitRootWrites() bool
	LeaderID() uint64
	ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error)
	Close() error
}

type rootRefreshBackend interface {
	Refresh() error
}

type rootTailBackend interface {
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	TailNotify() <-chan struct{}
	ObserveCommitted() (rootstorage.ObservedCommitted, error)
}

type rootSubmitBackend interface {
	CanSubmitRootWrites() bool
	LeaderID() uint64
}

type rootCoordinatorProtocolBackend interface {
	ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error)
}

type rootCloseBackend interface {
	Close() error
}

type rootBackendAdapter struct {
	rootBackend
	refresh  rootRefreshBackend
	tail     rootTailBackend
	submit   rootSubmitBackend
	protocol rootCoordinatorProtocolBackend
	closer   rootCloseBackend
}

func (a rootBackendAdapter) Refresh() error {
	if a.refresh == nil {
		return nil
	}
	return a.refresh.Refresh()
}

func (a rootBackendAdapter) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if a.tail == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return a.tail.WaitForTail(after, timeout)
}

func (a rootBackendAdapter) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if a.tail == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return a.tail.ObserveTail(after)
}

func (a rootBackendAdapter) TailNotify() <-chan struct{} {
	if a.tail == nil {
		return nil
	}
	return a.tail.TailNotify()
}

func (a rootBackendAdapter) ObserveCommitted() (rootstorage.ObservedCommitted, error) {
	if a.tail == nil {
		return rootstorage.ObservedCommitted{}, nil
	}
	return a.tail.ObserveCommitted()
}

func (a rootBackendAdapter) CanSubmitRootWrites() bool {
	if a.submit != nil {
		return a.submit.CanSubmitRootWrites()
	}
	return true
}

func (a rootBackendAdapter) LeaderID() uint64 {
	if a.submit != nil {
		return a.submit.LeaderID()
	}
	return 0
}

func (a rootBackendAdapter) ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	if a.protocol == nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, errGrantCommandUnsupported
	}
	return a.protocol.ApplyGrant(ctx, cmd)
}

func (a rootBackendAdapter) Close() error {
	if a.closer == nil {
		return nil
	}
	return a.closer.Close()
}

type rootBackendCapabilities struct {
	tail     bool
	protocol bool
}

func adaptRootBackend(root rootBackend) (rootRuntimeBackend, rootBackendCapabilities) {
	if runtime, ok := root.(rootRuntimeBackend); ok {
		return runtime, rootBackendCapabilities{tail: true, protocol: true}
	}
	adapter := rootBackendAdapter{rootBackend: root}
	if refresh, ok := root.(rootRefreshBackend); ok {
		adapter.refresh = refresh
	}
	if tail, ok := root.(rootTailBackend); ok {
		adapter.tail = tail
	}
	if submit, ok := root.(rootSubmitBackend); ok {
		adapter.submit = submit
	}
	if protocol, ok := root.(rootCoordinatorProtocolBackend); ok {
		adapter.protocol = protocol
	}
	if closer, ok := root.(rootCloseBackend); ok {
		adapter.closer = closer
	}
	return adapter, rootBackendCapabilities{
		tail:     adapter.tail != nil,
		protocol: adapter.protocol != nil,
	}
}

// OpenRootStore opens a Coordinator storage backend backed by the metadata root.
func OpenRootStore(root rootBackend) (*RootStore, error) {
	adapted, caps := adaptRootBackend(root)
	store := &RootStore{
		root:             adapted,
		supportsTail:     caps.tail,
		supportsProtocol: caps.protocol,
	}
	if err := store.reload(); err != nil {
		return nil, err
	}
	return store, nil
}

// RootStore persists Coordinator truth on top of the metadata root and reconstructs the
// region catalog by replaying committed root events.
type RootStore struct {
	root             rootRuntimeBackend
	supportsTail     bool
	supportsProtocol bool

	mu       sync.RWMutex
	snapshot Snapshot
}

// Load returns the last reconstructed snapshot.
func (s *RootStore) Load() (Snapshot, error) {
	if s == nil {
		return Snapshot{Descriptors: make(map[uint64]topology.Descriptor)}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneSnapshot(s.snapshot), nil
}

// Refresh reloads the reconstructed Coordinator snapshot from the underlying metadata root.
func (s *RootStore) Refresh() error {
	if s == nil {
		return nil
	}
	if !s.supportsTail {
		return s.reload()
	}
	return s.runAndReload(s.root.Refresh)
}

func (s *RootStore) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if s == nil || s.root == nil || !s.supportsTail {
		return rootstorage.TailAdvance{}, nil
	}
	advance, err := s.root.WaitForTail(after, timeout)
	if err != nil {
		return advance, err
	}
	s.applyTailAdvance(advance)
	return advance, nil
}

// ObserveTail observes the current rooted tail relative to after while keeping
// the cached rooted snapshot in sync whenever the observed advance requires a
// state reload or bootstrap install.
func (s *RootStore) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if s == nil || s.root == nil || !s.supportsTail {
		return rootstorage.TailAdvance{}, nil
	}
	advance, err := s.root.ObserveTail(after)
	if err != nil {
		return advance, err
	}
	s.applyTailAdvance(advance)
	return advance, nil
}

// SubscribeTail returns one rooted tail subscription. The subscription keeps
// its own acknowledged token and routes both watch-first observation and wait
// fallback through RootStore so callers no longer have to open-code tail-token
// loops or manage cache refresh themselves.
func (s *RootStore) SubscribeTail(after rootstorage.TailToken) *rootstorage.TailSubscription {
	if s == nil || s.root == nil || !s.supportsTail {
		return nil
	}
	return rootstorage.NewWatchedTailSubscription(after, s.ObserveTail, s.root.TailNotify(), s.WaitForTail)
}

func (s *RootStore) CanSubmitRootWrites() bool {
	if s == nil || s.root == nil {
		return true
	}
	return s.root.CanSubmitRootWrites()
}

func (s *RootStore) LeaderID() uint64 {
	if s == nil || s.root == nil {
		return 0
	}
	return s.root.LeaderID()
}

// AppendRootEvent persists one explicit rooted metadata event.
func (s *RootStore) AppendRootEvent(ctx context.Context, event rootevent.Event) error {
	if s == nil || s.root == nil || event.Kind == rootevent.KindUnknown {
		return nil
	}
	return s.runAndReload(func() error {
		_, err := s.root.Append(ctx, event)
		return err
	})
}

// SaveAllocatorState raises allocator fences in the metadata root.
func (s *RootStore) SaveAllocatorState(ctx context.Context, idCurrent, tsCurrent uint64) error {
	if s == nil {
		return nil
	}
	return s.runAndReload(func() error {
		if _, err := s.root.FenceAllocator(ctx, rootstate.AllocatorKindID, idCurrent); err != nil {
			return err
		}
		if _, err := s.root.FenceAllocator(ctx, rootstate.AllocatorKindTSO, tsCurrent); err != nil {
			return err
		}
		return nil
	})
}

func (s *RootStore) ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	if s == nil || s.root == nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, nil
	}
	if !s.supportsProtocol {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, errGrantCommandUnsupported
	}
	var cert rootproto.GrantCertificate
	state, err := s.applyAndReload(func() (rootstate.EunomiaState, error) {
		var protocolState rootstate.EunomiaState
		var applyErr error
		protocolState, cert, applyErr = s.root.ApplyGrant(ctx, cmd)
		return protocolState, applyErr
	})
	return state, cert, err
}

// Close releases storage resources.
func (s *RootStore) Close() error {
	if s == nil {
		return nil
	}
	return s.root.Close()
}

func (s *RootStore) reload() error {
	if s == nil || s.root == nil {
		return nil
	}
	if s.supportsTail {
		observed, err := s.root.ObserveCommitted()
		if err != nil {
			return err
		}
		s.replaceObserved(observed, rootstorage.TailToken{Cursor: observed.LastCursor()})
		return nil
	}
	snapshot, err := s.root.Snapshot()
	if err != nil {
		return err
	}
	out := SnapshotFromRoot(snapshot)
	out.CatchUpState = CatchUpStateFresh
	s.mu.Lock()
	out = PreserveNewerAuthorityState(out, s.snapshot)
	s.snapshot = out
	s.mu.Unlock()
	return nil
}

func (s *RootStore) runAndReload(run func() error) error {
	if s == nil {
		return nil
	}
	if run != nil {
		if err := run(); err != nil {
			return err
		}
	}
	return s.reload()
}

func (s *RootStore) applyAndReload(run func() (rootstate.EunomiaState, error)) (rootstate.EunomiaState, error) {
	if s == nil {
		return rootstate.EunomiaState{}, nil
	}
	if run == nil {
		return rootstate.EunomiaState{}, nil
	}
	protocolState, err := run()
	if eunomiaStatePresent(protocolState) {
		// Meta-root returns the authoritative Eunomia state even for HELD
		// rejections. Merge it before returning the error so a losing
		// coordinator stops campaigning on a stale local grant mirror.
		s.mergeEunomiaState(protocolState)
	}
	if err != nil {
		return protocolState, err
	}
	if err := s.reload(); err != nil {
		return protocolState, err
	}
	if eunomiaStatePresent(protocolState) {
		s.mergeEunomiaState(protocolState)
	}
	return protocolState, nil
}

func eunomiaStatePresent(state rootstate.EunomiaState) bool {
	return state.ActiveGrant.Present() ||
		len(state.RetiredGrants) > 0 ||
		len(state.GrantInheritances) > 0 ||
		state.RetiredEraFloor != 0
}

// mergeEunomiaState overlays the committed authority grant lifecycle from an
// authoritative Apply response onto the cached snapshot. Other fields
// (descriptors, allocator fences) are left untouched — the subsequent reload
// or a later tail advance refreshes them.
func (s *RootStore) mergeEunomiaState(state rootstate.EunomiaState) {
	if s == nil {
		return
	}
	incoming := Snapshot{
		ActiveGrant:       state.ActiveGrant,
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), state.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), state.GrantInheritances...),
		RetiredEraFloor:   state.RetiredEraFloor,
	}
	s.mu.Lock()
	merged := PreserveNewerAuthorityState(incoming, s.snapshot)
	s.snapshot.ActiveGrant = merged.ActiveGrant
	s.snapshot.RetiredGrants = append([]rootproto.GrantRetirement(nil), merged.RetiredGrants...)
	s.snapshot.GrantInheritances = append([]rootproto.GrantInheritance(nil), merged.GrantInheritances...)
	s.snapshot.RetiredEraFloor = merged.RetiredEraFloor
	s.mu.Unlock()
}

func (s *RootStore) replaceObserved(observed rootstorage.ObservedCommitted, token rootstorage.TailToken) {
	if s == nil {
		return
	}
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	out := SnapshotFromRoot(bootstrap.Snapshot)
	if token.Cursor.Term == 0 && token.Cursor.Index == 0 {
		token.Cursor = observed.LastCursor()
	}
	out.RootToken = token
	out.CatchUpState = CatchUpStateFresh
	s.mu.Lock()
	out = PreserveNewerAuthorityState(out, s.snapshot)
	s.snapshot = out
	s.mu.Unlock()
}

func (s *RootStore) applyTailAdvance(advance rootstorage.TailAdvance) {
	if s == nil {
		return
	}
	state := catchUpStateFromAdvance(advance)
	if !advance.ShouldReloadState() {
		s.mu.Lock()
		s.snapshot.CatchUpState = state
		s.mu.Unlock()
		return
	}
	bootstrap := rootmaterialize.BootstrapFromObserved(advance.Observed)
	out := SnapshotFromRoot(bootstrap.Snapshot)
	token := advance.Token
	if token.Cursor.Term == 0 && token.Cursor.Index == 0 {
		token.Cursor = advance.Observed.LastCursor()
	}
	out.RootToken = token
	out.CatchUpState = state
	s.mu.Lock()
	out = PreserveNewerAuthorityState(out, s.snapshot)
	s.snapshot = out
	s.mu.Unlock()
}

func catchUpStateFromAdvance(advance rootstorage.TailAdvance) CatchUpState {
	switch advance.CatchUpAction() {
	case rootstorage.TailCatchUpInstallBootstrap:
		return CatchUpStateBootstrapRequired
	case rootstorage.TailCatchUpRefreshState:
		return CatchUpStateLagging
	case rootstorage.TailCatchUpAcknowledgeWindow, rootstorage.TailCatchUpIdle:
		return CatchUpStateFresh
	default:
		return CatchUpStateUnspecified
	}
}
