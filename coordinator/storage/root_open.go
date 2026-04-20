package storage

import (
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"time"
)

type rootBackend interface {
	Snapshot() (rootstate.Snapshot, error)
	Append(events ...rootevent.Event) (rootstate.CommitInfo, error)
	FenceAllocator(kind rootstate.AllocatorKind, min uint64) (uint64, error)
}

type rootRefresher interface{ Refresh() error }

type rootTailWaiter interface {
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
}

type rootTailObserver interface {
	ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
}

type rootTailNotifier interface{ TailNotify() <-chan struct{} }

type rootObservedReader interface {
	ObserveCommitted() (rootstorage.ObservedCommitted, error)
}

type rootLeaderReader interface {
	IsLeader() bool
	LeaderID() uint64
}

type rootLeaseApplier interface {
	ApplyCoordinatorLease(cmd rootstate.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error)
}

type rootClosureApplier interface {
	ApplyCoordinatorClosure(cmd rootstate.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error)
}

// OpenRootStore opens a Coordinator storage backend backed by the metadata root.
func OpenRootStore(root rootBackend) (*RootStore, error) {
	store := &RootStore{root: root}
	if refresher, ok := root.(rootRefresher); ok {
		store.refresh = refresher.Refresh
	}
	if waiter, ok := root.(rootTailWaiter); ok {
		store.waitForTail = waiter.WaitForTail
	}
	if observer, ok := root.(rootTailObserver); ok {
		store.observeTail = observer.ObserveTail
	}
	if notifier, ok := root.(rootTailNotifier); ok {
		store.tailNotify = notifier.TailNotify
	}
	if observer, ok := root.(rootObservedReader); ok {
		store.observeCommitted = observer.ObserveCommitted
	}
	if leader, ok := root.(rootLeaderReader); ok {
		store.isLeader = leader.IsLeader
		store.leaderID = leader.LeaderID
	}
	if leaseApplier, ok := root.(rootLeaseApplier); ok {
		store.applyCoordinatorLease = leaseApplier.ApplyCoordinatorLease
	}
	if closureApplier, ok := root.(rootClosureApplier); ok {
		store.applyCoordinatorClosure = closureApplier.ApplyCoordinatorClosure
	}
	if err := store.reload(); err != nil {
		return nil, err
	}
	return store, nil
}

// OpenRootLocalStore opens a Coordinator storage backend backed by the local metadata
// root files in workdir.
func OpenRootLocalStore(workdir string) (*RootStore, error) {
	root, err := rootlocal.Open(workdir, nil)
	if err != nil {
		return nil, err
	}
	return OpenRootStore(root)
}
