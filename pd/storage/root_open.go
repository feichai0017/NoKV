package storage

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"time"
)

type rootBackend interface {
	Snapshot() (rootstate.Snapshot, error)
	Append(events ...rootevent.Event) (rootstate.CommitInfo, error)
	FenceAllocator(kind rootpkg.AllocatorKind, min uint64) (uint64, error)
}

// OpenRootStore opens a PD storage backend backed by the metadata root.
func OpenRootStore(root rootBackend) (*RootStore, error) {
	store := &RootStore{root: root}
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
		store.observe = observer.ObserveCommitted
	}
	if leader, ok := root.(interface {
		IsLeader() bool
		LeaderID() uint64
	}); ok {
		store.isLeader = leader.IsLeader
		store.leaderID = leader.LeaderID
	}
	if err := store.reload(); err != nil {
		return nil, err
	}
	return store, nil
}

// OpenRootLocalStore opens a PD storage backend backed by the local metadata
// root files in workdir.
func OpenRootLocalStore(workdir string) (*RootStore, error) {
	root, err := rootlocal.Open(workdir, nil)
	if err != nil {
		return nil, err
	}
	return OpenRootStore(root)
}
