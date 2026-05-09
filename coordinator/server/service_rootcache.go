package server

import (
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/meta/topology"
)

func (s *Service) reloadRootedView(refresh bool) (rootview.Snapshot, error) {
	if s == nil || s.storage == nil {
		return rootview.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)}, nil
	}
	if refresh {
		if err := s.storage.Refresh(); err != nil {
			return rootview.Snapshot{}, err
		}
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return rootview.Snapshot{}, err
	}
	s.publishRootSnapshot(snapshot)
	s.rootViewMu.RLock()
	if s.rootView.loaded {
		snapshot = rootview.CloneSnapshot(s.rootView.snapshot)
	}
	s.rootViewMu.RUnlock()
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
	var idGranted, tsoGranted bool
	for _, grant := range snapshot.ActiveGrants {
		grantID, grantTSO := s.installGrantAllocatorWindowsLocked(grant)
		idGranted = idGranted || grantID
		tsoGranted = tsoGranted || grantTSO
	}
	if !idGranted {
		s.fenceIDFromStorage(snapshot.Allocator.IDCurrent)
	}
	if !tsoGranted {
		s.fenceTSOFromStorage(snapshot.Allocator.TSCurrent)
	}
	s.setLastRootReload(nil)
	return nil
}

func (s *Service) refreshGrantMirror(snapshot rootview.Snapshot) {
	if s == nil {
		return
	}
	s.grantMu.Lock()
	s.grantView.Refresh(snapshot)
	s.grantMu.Unlock()
}

func (s *Service) cacheGrantCertificate(cert rootproto.GrantCertificate) {
	if s == nil || !cert.Grant.Present() {
		return
	}
	s.grantMu.Lock()
	if grant, ok := s.grantView.GrantByID(cert.Grant.GrantID); ok && grantCertificateCoversGrant(cert, grant) {
		if s.grantView.certificates == nil {
			s.grantView.certificates = make(map[string]rootproto.GrantCertificate)
		}
		s.grantView.certificates[cert.Grant.GrantID] = cert
	}
	s.grantMu.Unlock()
}

func (s *Service) rootSnapshotRefreshWindow() time.Duration {
	if s == nil || s.rootViewTTL <= 0 {
		return defaultRootSnapshotRefreshInterval
	}
	return s.rootViewTTL
}

func (s *Service) shouldReplaceRootSnapshotLocked(snapshot rootview.Snapshot) bool {
	if !s.rootView.loaded {
		return true
	}
	current := s.rootView.snapshot.RootToken
	return !current.AdvancedSince(snapshot.RootToken)
}

func (s *Service) cacheRootSnapshot(snapshot rootview.Snapshot, refreshedAt time.Time) (rootview.Snapshot, bool) {
	if s == nil {
		return rootview.Snapshot{}, false
	}
	if refreshedAt.IsZero() {
		nowFn := s.now
		if nowFn == nil {
			nowFn = time.Now
		}
		refreshedAt = nowFn()
	}
	authoritySnapshot := s.currentAuthoritySnapshot()
	s.rootViewMu.Lock()
	updated := false
	snapshot = rootview.PreserveNewerAuthorityState(snapshot, authoritySnapshot)
	if s.shouldReplaceRootSnapshotLocked(snapshot) {
		if s.rootView.loaded {
			snapshot = rootview.PreserveNewerAuthorityState(snapshot, s.rootView.snapshot)
		}
		s.rootView.snapshot = rootview.CloneSnapshot(snapshot)
		s.rootView.loaded = true
		s.rootView.refreshedAt = refreshedAt
		updated = true
	}
	cached := rootview.CloneSnapshot(s.rootView.snapshot)
	s.rootViewMu.Unlock()
	return cached, updated
}

func (s *Service) refreshCurrentRootSnapshot(snapshot rootview.Snapshot) bool {
	if s == nil {
		return false
	}
	cached, updated := s.cacheRootSnapshot(snapshot, time.Time{})
	if !updated {
		return false
	}
	s.refreshGrantMirror(cached)
	s.setLastRootReload(nil)
	return true
}

func (s *Service) currentAuthoritySnapshot() rootview.Snapshot {
	if s == nil {
		return rootview.Snapshot{}
	}
	s.grantMu.RLock()
	defer s.grantMu.RUnlock()
	return rootview.Snapshot{
		ActiveGrants:      s.grantView.Grants(),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.grantView.retirements...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), s.grantView.inheritances...),
		RetiredEraFloor:   s.grantView.retiredEraFloor,
	}
}

func (s *Service) cachedRootSnapshot() (rootview.Snapshot, bool) {
	if s == nil {
		return rootview.Snapshot{}, false
	}
	s.rootViewMu.RLock()
	defer s.rootViewMu.RUnlock()
	if !s.rootView.loaded {
		return rootview.Snapshot{}, false
	}
	return rootview.CloneSnapshot(s.rootView.snapshot), true
}

func (s *Service) publishEunomiaState(state rootstate.EunomiaState) {
	if s == nil || !serviceEunomiaStatePresent(state) {
		return
	}
	snapshot := rootview.Snapshot{
		ActiveGrants:      state.ActiveGrants,
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), state.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), state.GrantInheritances...),
		RetiredEraFloor:   state.RetiredEraFloor,
	}
	s.refreshGrantMirror(snapshot)
	s.rootViewMu.Lock()
	if s.rootView.loaded {
		s.rootView.snapshot.ActiveGrants = snapshot.ActiveGrants
		s.rootView.snapshot.RetiredGrants = append([]rootproto.GrantRetirement(nil), snapshot.RetiredGrants...)
		s.rootView.snapshot.GrantInheritances = append([]rootproto.GrantInheritance(nil), snapshot.GrantInheritances...)
		s.rootView.snapshot.RetiredEraFloor = snapshot.RetiredEraFloor
	}
	s.rootViewMu.Unlock()
}

func serviceEunomiaStatePresent(state rootstate.EunomiaState) bool {
	return len(state.ActiveGrants) > 0 ||
		len(state.RetiredGrants) > 0 ||
		len(state.GrantInheritances) > 0 ||
		state.RetiredEraFloor != 0
}

func (s *Service) publishRootSnapshot(snapshot rootview.Snapshot) {
	if s == nil {
		return
	}
	if !s.refreshCurrentRootSnapshot(snapshot) {
		return
	}
	if s.cluster != nil {
		s.cluster.ReplaceRootSnapshot(rootstate.Snapshot{
			State: rootstate.State{
				ActiveGrants:      snapshot.ActiveGrants,
				RetiredGrants:     append([]rootproto.GrantRetirement(nil), snapshot.RetiredGrants...),
				GrantInheritances: append([]rootproto.GrantInheritance(nil), snapshot.GrantInheritances...),
				RetiredEraFloor:   snapshot.RetiredEraFloor,
			},
			Stores:              snapshot.Stores,
			Subtrees:            snapshot.Subtrees,
			Mounts:              snapshot.Mounts,
			Quotas:              snapshot.Quotas,
			Descriptors:         snapshot.Descriptors,
			PendingPeerChanges:  snapshot.PendingPeerChanges,
			PendingRangeChanges: snapshot.PendingRangeChanges,
		}, snapshot.RootToken)
	}
}

func (s *Service) currentRootSnapshot() (rootview.Snapshot, error) {
	if s == nil || s.storage == nil {
		return rootview.Snapshot{}, nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	s.rootViewMu.RLock()
	if s.rootView.loaded {
		snapshot := rootview.CloneSnapshot(s.rootView.snapshot)
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
		return rootview.Snapshot{}, err
	}
	s.refreshCurrentRootSnapshot(snapshot)
	return rootview.CloneSnapshot(snapshot), nil
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
