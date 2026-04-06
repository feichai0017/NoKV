package storage

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"sync"
	"time"
)

// RootStore persists PD truth on top of the metadata root and reconstructs the
// region catalog by replaying committed root events.
type RootStore struct {
	root        rootBackend
	refresh     func() error
	observeTail func(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	waitForTail func(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	tailNotify  func() <-chan struct{}
	observe     func() (rootstorage.ObservedCommitted, error)
	isLeader    func() bool
	leaderID    func() uint64
	campaign    func() error

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

// Refresh reloads the reconstructed PD snapshot from the underlying metadata root.
func (s *RootStore) Refresh() error {
	if s == nil {
		return nil
	}
	if s.refresh != nil {
		if err := s.refresh(); err != nil {
			return err
		}
	}
	return s.reload()
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
	if advance.ShouldReloadState() {
		s.replaceObserved(advance.Observed)
	}
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
	if advance.ShouldReloadState() {
		s.replaceObserved(advance.Observed)
	}
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

func (s *RootStore) Campaign() error {
	if s == nil || s.root == nil || s.campaign == nil {
		return nil
	}
	return s.campaign()
}

// AppendRootEvent persists one explicit rooted metadata event.
func (s *RootStore) AppendRootEvent(event rootevent.Event) error {
	if s == nil || s.root == nil || event.Kind == rootevent.KindUnknown {
		return nil
	}
	if _, err := s.root.Append(event); err != nil {
		return err
	}
	return s.reload()
}

// SaveAllocatorState raises allocator fences in the metadata root.
func (s *RootStore) SaveAllocatorState(idCurrent, tsCurrent uint64) error {
	if s == nil {
		return nil
	}
	if _, err := s.root.FenceAllocator(rootpkg.AllocatorKindID, idCurrent); err != nil {
		return err
	}
	if _, err := s.root.FenceAllocator(rootpkg.AllocatorKindTSO, tsCurrent); err != nil {
		return err
	}
	return s.reload()
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
	if s.observe != nil {
		observed, err := s.observe()
		if err != nil {
			return err
		}
		s.replaceObserved(observed)
		return nil
	}
	snapshot, err := s.root.Snapshot()
	if err != nil {
		return err
	}
	out := SnapshotFromRoot(snapshot)
	s.mu.Lock()
	s.snapshot = out
	s.mu.Unlock()
	return nil
}

func (s *RootStore) replaceObserved(observed rootstorage.ObservedCommitted) {
	if s == nil {
		return
	}
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	out := SnapshotFromRoot(bootstrap.Snapshot)
	s.mu.Lock()
	s.snapshot = out
	s.mu.Unlock()
}
