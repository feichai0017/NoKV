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
	errCoordinatorLeaseCommandUnsupported   = errors.New("coordinator/rootview: coordinator lease command unsupported")
	errCoordinatorClosureCommandUnsupported = errors.New("coordinator/rootview: coordinator closure command unsupported")
)

// RootStorage persists control-plane mutations into durable metadata truth and
// exposes the reconstructed rooted snapshot back to Coordinator.
type RootStorage interface {
	Load() (Snapshot, error)
	AppendRootEvent(ctx context.Context, event rootevent.Event) error
	SaveAllocatorState(ctx context.Context, idCurrent, tsCurrent uint64) error
	ApplyCoordinatorLease(ctx context.Context, cmd rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error)
	ApplyCoordinatorClosure(ctx context.Context, cmd rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error)
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

type rootOptionalBackend interface {
	rootBackend
	Refresh() error
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	TailNotify() <-chan struct{}
	ObserveCommitted() (rootstorage.ObservedCommitted, error)
	IsLeader() bool
	LeaderID() uint64
	ApplyCoordinatorLease(ctx context.Context, cmd rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error)
	ApplyCoordinatorClosure(ctx context.Context, cmd rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error)
}

// OpenRootStore opens a Coordinator storage backend backed by the metadata root.
func OpenRootStore(root rootBackend) (*RootStore, error) {
	store := &RootStore{root: root}
	if optional, ok := root.(rootOptionalBackend); ok {
		store.refresh = optional.Refresh
		store.waitForTail = optional.WaitForTail
		store.observeTail = optional.ObserveTail
		store.tailNotify = optional.TailNotify
		store.observeCommitted = optional.ObserveCommitted
		store.isLeader = optional.IsLeader
		store.leaderID = optional.LeaderID
		store.applyCoordinatorLease = optional.ApplyCoordinatorLease
		store.applyCoordinatorClosure = optional.ApplyCoordinatorClosure
	} else {
		if refresher, ok := root.(interface{ Refresh() error }); ok {
			store.refresh = refresher.Refresh
		}
		if waiter, ok := root.(interface {
			WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
		}); ok {
			store.waitForTail = waiter.WaitForTail
		}
		if observer, ok := root.(interface {
			ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
		}); ok {
			store.observeTail = observer.ObserveTail
		}
		if notifier, ok := root.(interface{ TailNotify() <-chan struct{} }); ok {
			store.tailNotify = notifier.TailNotify
		}
		if observer, ok := root.(interface {
			ObserveCommitted() (rootstorage.ObservedCommitted, error)
		}); ok {
			store.observeCommitted = observer.ObserveCommitted
		}
		if leader, ok := root.(interface {
			IsLeader() bool
			LeaderID() uint64
		}); ok {
			store.isLeader = leader.IsLeader
			store.leaderID = leader.LeaderID
		}
		if leaseApplier, ok := root.(interface {
			ApplyCoordinatorLease(ctx context.Context, cmd rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error)
		}); ok {
			store.applyCoordinatorLease = leaseApplier.ApplyCoordinatorLease
		}
		if closureApplier, ok := root.(interface {
			ApplyCoordinatorClosure(ctx context.Context, cmd rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error)
		}); ok {
			store.applyCoordinatorClosure = closureApplier.ApplyCoordinatorClosure
		}
	}
	if err := store.reload(); err != nil {
		return nil, err
	}
	return store, nil
}

// RootStore persists Coordinator truth on top of the metadata root and reconstructs the
// region catalog by replaying committed root events.
type RootStore struct {
	root                    rootBackend
	refresh                 func() error
	observeTail             func(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	waitForTail             func(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	tailNotify              func() <-chan struct{}
	observeCommitted        func() (rootstorage.ObservedCommitted, error)
	isLeader                func() bool
	leaderID                func() uint64
	applyCoordinatorLease   func(ctx context.Context, cmd rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error)
	applyCoordinatorClosure func(ctx context.Context, cmd rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error)

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
	return s.runAndReload(s.refresh)
}

func (s *RootStore) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if s == nil || s.root == nil {
		return rootstorage.TailAdvance{}, nil
	}
	if s.waitForTail == nil {
		return rootstorage.TailAdvance{}, nil
	}
	advance, err := s.waitForTail(after, timeout)
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
	if s == nil || s.root == nil {
		return rootstorage.TailAdvance{}, nil
	}
	if s.observeTail == nil {
		return rootstorage.TailAdvance{}, nil
	}
	advance, err := s.observeTail(after)
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
	if s == nil || s.root == nil {
		return nil
	}
	if s.observeTail != nil {
		var watch <-chan struct{}
		if s.tailNotify != nil {
			watch = s.tailNotify()
		}
		return rootstorage.NewWatchedTailSubscription(after, s.ObserveTail, watch, s.WaitForTail)
	}
	if s.waitForTail == nil {
		return nil
	}
	return rootstorage.NewTailSubscription(after, s.WaitForTail)
}

func (s *RootStore) IsLeader() bool {
	if s == nil || s.root == nil {
		return true
	}
	if s.isLeader != nil {
		return s.isLeader()
	}
	return true
}

func (s *RootStore) LeaderID() uint64 {
	if s == nil || s.root == nil {
		return 0
	}
	if s.leaderID != nil {
		return s.leaderID()
	}
	return 0
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

func (s *RootStore) ApplyCoordinatorLease(ctx context.Context, cmd rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	if s == nil || s.root == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	if s.applyCoordinatorLease == nil {
		return rootstate.CoordinatorProtocolState{}, errCoordinatorLeaseCommandUnsupported
	}
	return s.applyAndReload(func() (rootstate.CoordinatorProtocolState, error) {
		return s.applyCoordinatorLease(ctx, cmd)
	})
}

func (s *RootStore) ApplyCoordinatorClosure(ctx context.Context, cmd rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	if s == nil || s.root == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	if s.applyCoordinatorClosure == nil {
		return rootstate.CoordinatorProtocolState{}, errCoordinatorClosureCommandUnsupported
	}
	return s.applyAndReload(func() (rootstate.CoordinatorProtocolState, error) {
		return s.applyCoordinatorClosure(ctx, cmd)
	})
}

// Close releases storage resources.
func (s *RootStore) Close() error {
	if s == nil {
		return nil
	}
	if closer, ok := s.root.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (s *RootStore) reload() error {
	if s == nil || s.root == nil {
		return nil
	}
	if s.observeCommitted != nil {
		observed, err := s.observeCommitted()
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

func (s *RootStore) applyAndReload(run func() (rootstate.CoordinatorProtocolState, error)) (rootstate.CoordinatorProtocolState, error) {
	if s == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	if run == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	protocolState, err := run()
	if err != nil {
		return protocolState, err
	}
	// The Apply response carries the authoritative post-apply
	// Lease/Seal/Closure from the meta-root leader. Merge it into the cached
	// snapshot BEFORE the reload roundtrip so subsequent calls never race
	// against a follower that has not yet replicated the event. Without this,
	// a coordinator that writes to the meta-root leader and then reads back
	// from a lagging follower observes a state regression and treats its own
	// fresh lease as stale, which triggers churn (lease lineage mismatches,
	// "lease held" retries) in multi-coordinator deployments.
	s.mergeCoordinatorProtocolState(protocolState)
	return protocolState, s.reload()
}

// mergeCoordinatorProtocolState overlays the Lease/Seal/Closure from an
// authoritative Apply response onto the cached snapshot. Other fields
// (descriptors, allocator fences) are left untouched — the subsequent reload
// or a later tail advance refreshes them.
func (s *RootStore) mergeCoordinatorProtocolState(state rootstate.CoordinatorProtocolState) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.snapshot.CoordinatorLease = state.Lease
	s.snapshot.CoordinatorSeal = state.Seal
	s.snapshot.CoordinatorClosure = state.Closure
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
