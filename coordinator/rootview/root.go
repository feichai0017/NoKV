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
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

var (
	errTenureCommandUnsupported   = errors.New("coordinator/rootview: coordinator tenure command unsupported")
	errHandoverCommandUnsupported = errors.New("coordinator/rootview: coordinator handover command unsupported")
)

// RootStorage persists control-plane mutations into durable metadata truth and
// exposes the reconstructed rooted snapshot back to Coordinator.
type RootStorage interface {
	Load() (Snapshot, error)
	AppendRootEvent(ctx context.Context, event rootevent.Event) error
	SaveAllocatorState(ctx context.Context, idCurrent, tsCurrent uint64) error
	ApplyTenure(ctx context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error)
	ApplyHandover(ctx context.Context, cmd rootproto.HandoverCommand) (rootstate.SuccessionState, error)
	Refresh() error
	IsLeader() bool
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
	IsLeader() bool
	LeaderID() uint64
	ApplyTenure(ctx context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error)
	ApplyHandover(ctx context.Context, cmd rootproto.HandoverCommand) (rootstate.SuccessionState, error)
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

type rootLeaderBackend interface {
	IsLeader() bool
	LeaderID() uint64
}

type rootCoordinatorProtocolBackend interface {
	ApplyTenure(ctx context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error)
	ApplyHandover(ctx context.Context, cmd rootproto.HandoverCommand) (rootstate.SuccessionState, error)
}

type rootCloseBackend interface {
	Close() error
}

type rootBackendAdapter struct {
	rootBackend
	refresh  rootRefreshBackend
	tail     rootTailBackend
	leader   rootLeaderBackend
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

func (a rootBackendAdapter) IsLeader() bool {
	if a.leader == nil {
		return true
	}
	return a.leader.IsLeader()
}

func (a rootBackendAdapter) LeaderID() uint64 {
	if a.leader == nil {
		return 0
	}
	return a.leader.LeaderID()
}

func (a rootBackendAdapter) ApplyTenure(ctx context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	if a.protocol == nil {
		return rootstate.SuccessionState{}, errTenureCommandUnsupported
	}
	return a.protocol.ApplyTenure(ctx, cmd)
}

func (a rootBackendAdapter) ApplyHandover(ctx context.Context, cmd rootproto.HandoverCommand) (rootstate.SuccessionState, error) {
	if a.protocol == nil {
		return rootstate.SuccessionState{}, errHandoverCommandUnsupported
	}
	return a.protocol.ApplyHandover(ctx, cmd)
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
	if leader, ok := root.(rootLeaderBackend); ok {
		adapter.leader = leader
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
		return Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
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

func (s *RootStore) IsLeader() bool {
	if s == nil || s.root == nil {
		return true
	}
	return s.root.IsLeader()
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

func (s *RootStore) ApplyTenure(ctx context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	if s == nil || s.root == nil {
		return rootstate.SuccessionState{}, nil
	}
	if !s.supportsProtocol {
		return rootstate.SuccessionState{}, errTenureCommandUnsupported
	}
	return s.applyAndReload(func() (rootstate.SuccessionState, error) {
		return s.root.ApplyTenure(ctx, cmd)
	})
}

func (s *RootStore) ApplyHandover(ctx context.Context, cmd rootproto.HandoverCommand) (rootstate.SuccessionState, error) {
	if s == nil || s.root == nil {
		return rootstate.SuccessionState{}, nil
	}
	if !s.supportsProtocol {
		return rootstate.SuccessionState{}, errHandoverCommandUnsupported
	}
	return s.applyAndReload(func() (rootstate.SuccessionState, error) {
		return s.root.ApplyHandover(ctx, cmd)
	})
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

func (s *RootStore) applyAndReload(run func() (rootstate.SuccessionState, error)) (rootstate.SuccessionState, error) {
	if s == nil {
		return rootstate.SuccessionState{}, nil
	}
	if run == nil {
		return rootstate.SuccessionState{}, nil
	}
	protocolState, err := run()
	if err != nil {
		return protocolState, err
	}
	// The Apply response carries the authoritative post-apply
	// Tenure/Legacy/Handover from the meta-root leader. Merge it into the cached
	// snapshot BEFORE the reload roundtrip so subsequent calls never race
	// against a follower that has not yet replicated the event. Without this,
	// a coordinator that writes to the meta-root leader and then reads back
	// from a lagging follower observes a state regression and treats its own
	// fresh lease as stale, which triggers churn (lease lineage mismatches,
	// "lease held" retries) in multi-coordinator deployments.
	s.mergeSuccessionState(protocolState)
	return protocolState, s.reload()
}

// mergeSuccessionState overlays the Tenure/Legacy/Handover from an
// authoritative Apply response onto the cached snapshot. Other fields
// (descriptors, allocator fences) are left untouched — the subsequent reload
// or a later tail advance refreshes them.
func (s *RootStore) mergeSuccessionState(state rootstate.SuccessionState) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.snapshot.Tenure = state.Tenure
	s.snapshot.Legacy = state.Legacy
	s.snapshot.Handover = state.Handover
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
