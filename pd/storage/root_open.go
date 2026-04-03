package storage

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"time"
)

type rootBackend interface {
	rootpkg.SnapshotReader
	rootpkg.EventWriter
	rootpkg.AllocatorFenceWriter
}

// OpenRootStore opens a PD storage backend backed by the metadata root.
func OpenRootStore(root rootBackend) (*RootStore, error) {
	store := &RootStore{root: root}
	if refresher, ok := root.(interface{ Refresh() error }); ok {
		store.refresh = refresher.Refresh
	}
	if waiter, ok := root.(interface {
		WaitForChange(after rootstate.Cursor, timeout time.Duration) (rootstate.Cursor, error)
	}); ok {
		store.waitForChange = waiter.WaitForChange
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
