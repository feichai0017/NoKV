package server

import (
	"time"

	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

func (s *Service) reloadRootedView(refresh bool) (coordstorage.Snapshot, error) {
	if s == nil || s.storage == nil {
		return coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
	}
	if refresh {
		if err := s.storage.Refresh(); err != nil {
			return coordstorage.Snapshot{}, err
		}
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return coordstorage.Snapshot{}, err
	}
	s.publishRootSnapshot(snapshot)
	return snapshot, nil
}

func (s *Service) reloadAndFenceAllocators(refresh bool) error {
	snapshot, err := s.reloadRootedView(refresh)
	if err != nil {
		s.setLastRootReload(err)
		return err
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()
	s.fenceIDFromStorage(snapshot.Allocator.IDCurrent)
	s.fenceTSOFromStorage(snapshot.Allocator.TSCurrent)
	s.setLastRootReload(nil)
	return nil
}

func (s *Service) refreshLeaseMirror(snapshot coordstorage.Snapshot) {
	if s == nil {
		return
	}
	s.leaseMu.Lock()
	s.leaseView.Refresh(snapshot)
	s.leaseMu.Unlock()
}

func (s *Service) rootSnapshotRefreshWindow() time.Duration {
	if s == nil || s.rootViewTTL <= 0 {
		return defaultRootSnapshotRefreshInterval
	}
	return s.rootViewTTL
}

func (s *Service) shouldReplaceRootSnapshotLocked(snapshot coordstorage.Snapshot) bool {
	if !s.rootView.loaded {
		return true
	}
	current := s.rootView.snapshot.RootToken
	return !current.AdvancedSince(snapshot.RootToken)
}

func (s *Service) cacheRootSnapshot(snapshot coordstorage.Snapshot, refreshedAt time.Time) bool {
	if s == nil {
		return false
	}
	if refreshedAt.IsZero() {
		nowFn := s.now
		if nowFn == nil {
			nowFn = time.Now
		}
		refreshedAt = nowFn()
	}
	s.rootViewMu.Lock()
	updated := false
	if s.shouldReplaceRootSnapshotLocked(snapshot) {
		s.rootView.snapshot = coordstorage.CloneSnapshot(snapshot)
		s.rootView.loaded = true
		s.rootView.refreshedAt = refreshedAt
		updated = true
	}
	s.rootViewMu.Unlock()
	return updated
}

func (s *Service) refreshCurrentRootSnapshot(snapshot coordstorage.Snapshot) bool {
	if s == nil {
		return false
	}
	if !s.cacheRootSnapshot(snapshot, time.Time{}) {
		return false
	}
	s.refreshLeaseMirror(snapshot)
	s.setLastRootReload(nil)
	return true
}

func (s *Service) publishRootSnapshot(snapshot coordstorage.Snapshot) {
	if s == nil {
		return
	}
	if !s.refreshCurrentRootSnapshot(snapshot) {
		return
	}
	if s.cluster != nil {
		s.cluster.ReplaceRootSnapshot(snapshot.Descriptors, snapshot.PendingPeerChanges, snapshot.PendingRangeChanges, snapshot.RootToken)
	}
}

func (s *Service) currentRootSnapshot() (coordstorage.Snapshot, error) {
	if s == nil || s.storage == nil {
		return coordstorage.Snapshot{}, nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	s.rootViewMu.RLock()
	if s.rootView.loaded {
		snapshot := coordstorage.CloneSnapshot(s.rootView.snapshot)
		stale := now.Sub(s.rootView.refreshedAt) > s.rootSnapshotRefreshWindow()
		s.rootViewMu.RUnlock()
		if stale {
			s.maybeRefreshRootSnapshotAsync()
		}
		return snapshot, nil
	}
	s.rootViewMu.RUnlock()

	snapshot, err := s.storage.Load()
	if err != nil {
		s.setLastRootReload(err)
		return coordstorage.Snapshot{}, err
	}
	s.refreshCurrentRootSnapshot(snapshot)
	return coordstorage.CloneSnapshot(snapshot), nil
}

func (s *Service) maybeRefreshRootSnapshotAsync() {
	if s == nil || s.storage == nil {
		return
	}
	s.rootViewMu.Lock()
	if s.rootView.refreshing {
		s.rootViewMu.Unlock()
		return
	}
	s.rootView.refreshing = true
	s.rootViewMu.Unlock()
	go func() {
		defer func() {
			s.rootViewMu.Lock()
			s.rootView.refreshing = false
			s.rootViewMu.Unlock()
		}()
		snapshot, err := s.storage.Load()
		if err != nil {
			s.setLastRootReload(err)
			return
		}
		s.refreshCurrentRootSnapshot(snapshot)
	}()
}

func (s *Service) cachedRootSnapshotStale() bool {
	if s == nil {
		return false
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	s.rootViewMu.RLock()
	defer s.rootViewMu.RUnlock()
	if !s.rootView.loaded {
		return false
	}
	return now.Sub(s.rootView.refreshedAt) > s.rootSnapshotRefreshWindow()
}

func (s *Service) lastRootReloadError() string {
	if s == nil {
		return ""
	}
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.lastRootError
}

func (s *Service) setLastRootReload(err error) {
	if s == nil {
		return
	}
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	if err != nil {
		s.lastRootError = err.Error()
		return
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	s.lastRootReload = nowFn().UnixNano()
	s.lastRootError = ""
}
