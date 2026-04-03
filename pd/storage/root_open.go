package storage

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
)

type rootBackend interface {
	rootpkg.StateReader
	rootpkg.EventAppender
	rootpkg.AllocatorFencer
}

type refreshableRoot interface {
	Refresh() error
}

type leaderAwareRoot interface {
	IsLeader() bool
	LeaderID() uint64
}

// OpenRootStore opens a PD storage backend backed by the metadata root.
func OpenRootStore(root rootBackend) (*RootStore, error) {
	store := &RootStore{root: root}
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
