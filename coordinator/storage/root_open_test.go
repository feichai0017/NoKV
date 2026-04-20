package storage

import (
	"context"
	"errors"
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func campaignLease(store *RootStore, holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64, predecessorDigest string) (rootstate.CoordinatorLease, error) {
	state, err := store.ApplyCoordinatorLease(rootstate.CoordinatorLeaseCommand{
		Kind:              rootstate.CoordinatorLeaseCommandIssue,
		HolderID:          holderID,
		ExpiresUnixNano:   expiresUnixNano,
		NowUnixNano:       nowUnixNano,
		PredecessorDigest: predecessorDigest,
		HandoffFrontiers:  controlplane.Frontiers(idFence, tsoFence, descriptorRevision),
	})
	return state.Lease, err
}

func releaseLease(store *RootStore, holderID string, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error) {
	state, err := store.ApplyCoordinatorLease(rootstate.CoordinatorLeaseCommand{
		Kind:             rootstate.CoordinatorLeaseCommandRelease,
		HolderID:         holderID,
		NowUnixNano:      nowUnixNano,
		HandoffFrontiers: controlplane.Frontiers(idFence, tsoFence, 0),
	})
	return state.Lease, err
}

func sealLease(store *RootStore, holderID string, nowUnixNano int64, frontiers rootstate.CoordinatorDutyFrontiers) (rootstate.CoordinatorSeal, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandSeal,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	return state.Seal, err
}

func confirmClosure(store *RootStore, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandConfirm,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func closeClosure(store *RootStore, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandClose,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func reattachClosure(store *RootStore, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandReattach,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func TestOpenRootLocalStoreCreatesMetadataRootFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenRootLocalStore(dir)
	require.NoError(t, err)

	require.NoError(t, store.SaveAllocatorState(9, 17))

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(9), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(17), snapshot.Allocator.TSCurrent)

	require.FileExists(t, filepath.Join(dir, rootfile.CheckpointFileName))
}

func TestRootStoreObserveTailRefreshesCachedSnapshot(t *testing.T) {
	initial := observedDescriptorsSnapshot(testDescriptor(91, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 1})
	updated := observedDescriptorsTailSnapshot(
		testDescriptor(92, []byte("m"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil),
		rootstate.Cursor{Term: 1, Index: 1},
		rootstate.Cursor{Term: 1, Index: 2},
	)
	backend := &stubRootBackend{observed: initial}
	backend.observeTailFn = func(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
		return updated.Advance(after, rootstorage.TailToken{Cursor: updated.LastCursor(), Revision: 1}), nil
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Contains(t, snapshot.Descriptors, uint64(91))

	advance, err := store.ObserveTail(rootstorage.TailToken{Cursor: initial.LastCursor()})
	require.NoError(t, err)
	require.True(t, advance.ShouldReloadState())

	snapshot, err = store.Load()
	require.NoError(t, err)
	require.NotContains(t, snapshot.Descriptors, uint64(91))
	require.Contains(t, snapshot.Descriptors, uint64(92))
	require.Equal(t, uint64(1), snapshot.RootToken.Revision)
	require.Equal(t, updated.LastCursor(), snapshot.RootToken.Cursor)
	require.Equal(t, CatchUpStateLagging, snapshot.CatchUpState)
}

func TestRootStoreWaitForTailRefreshesCachedSnapshot(t *testing.T) {
	initial := observedDescriptorsSnapshot(testDescriptor(101, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 1})
	updated := observedDescriptorsTailSnapshot(
		testDescriptor(102, []byte("m"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil),
		rootstate.Cursor{Term: 1, Index: 1},
		rootstate.Cursor{Term: 1, Index: 2},
	)
	backend := &stubRootBackend{observed: initial}
	backend.waitForTailFn = func(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
		return updated.Advance(after, rootstorage.TailToken{Cursor: updated.LastCursor(), Revision: 1}), nil
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	advance, err := store.WaitForTail(rootstorage.TailToken{Cursor: initial.LastCursor()}, time.Second)
	require.NoError(t, err)
	require.True(t, advance.ShouldReloadState())

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Contains(t, snapshot.Descriptors, uint64(102))
	require.Equal(t, CatchUpStateLagging, snapshot.CatchUpState)
}

func TestRootStoreObserveTailMarksBootstrapRequired(t *testing.T) {
	initial := observedDescriptorsSnapshot(testDescriptor(131, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 1})
	updated := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{
			Snapshot: rootstate.Snapshot{
				State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 2, Index: 9}},
				Descriptors: map[uint64]descriptor.Descriptor{
					132: testDescriptor(132, []byte("m"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil),
				},
				PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
				PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
			},
			TailOffset: 20,
		},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 1,
			StartOffset:     20,
			EndOffset:       20,
		},
	}
	backend := &stubRootBackend{observed: initial}
	backend.observeTailFn = func(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
		return updated.Advance(after, rootstorage.TailToken{Cursor: updated.LastCursor(), Revision: 7}), nil
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	advance, err := store.ObserveTail(rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 1}, Revision: 1})
	require.NoError(t, err)
	require.True(t, advance.NeedsBootstrapInstall())

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, CatchUpStateBootstrapRequired, snapshot.CatchUpState)
	require.Equal(t, uint64(7), snapshot.RootToken.Revision)
	require.Contains(t, snapshot.Descriptors, uint64(132))
}

func TestRootStoreSubscribeTailNextRefreshesCachedSnapshot(t *testing.T) {
	initial := observedDescriptorsSnapshot(testDescriptor(111, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 1})
	updated := observedDescriptorsSnapshot(testDescriptor(112, []byte("m"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 2})
	notify := make(chan struct{}, 1)
	var phase int32
	backend := &stubRootBackend{observed: initial, tailNotifyCh: notify}
	backend.observeTailFn = func(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
		if atomic.LoadInt32(&phase) == 0 {
			return initial.Advance(after, after), nil
		}
		return updated.Advance(after, rootstorage.TailToken{Cursor: updated.LastCursor(), Revision: 1}), nil
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)
	subscription := store.SubscribeTail(rootstorage.TailToken{})
	require.NotNil(t, subscription)

	go func() {
		time.Sleep(20 * time.Millisecond)
		atomic.StoreInt32(&phase, 1)
		notify <- struct{}{}
	}()

	advance, err := subscription.Next(context.Background(), 200*time.Millisecond)
	require.NoError(t, err)
	require.True(t, advance.ShouldReloadState())

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Contains(t, snapshot.Descriptors, uint64(112))
}

func TestRootStoreLeadershipAndCloseDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(121, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		isLeaderValue: false,
		leaderIDValue: 9,
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)
	require.False(t, store.IsLeader())
	require.Equal(t, uint64(9), store.LeaderID())
	require.NoError(t, store.Close())
	require.Equal(t, 1, backend.closeCalls)
}

func TestRootStoreCampaignCoordinatorLeaseDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(124, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		lease: rootstate.CoordinatorLease{
			HolderID:        "c1",
			ExpiresUnixNano: 1_000,
		},
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	lease, err := campaignLease(store, "c1", 1_000, 100, 12, 34, 56, "")
	require.NoError(t, err)
	require.Equal(t, backend.lease, lease)
	require.Equal(t, 1, backend.leaseCampaignCalls)
}

func TestRootStoreReleaseCoordinatorLeaseDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(125, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		lease: rootstate.CoordinatorLease{
			HolderID:        "c1",
			ExpiresUnixNano: 200,
		},
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	lease, err := releaseLease(store, "c1", 200, 12, 34)
	require.NoError(t, err)
	require.Equal(t, backend.lease, lease)
	require.Equal(t, 1, backend.leaseReleaseCalls)
}

func TestRootStoreSealCoordinatorLeaseDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(126, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		seal: rootstate.CoordinatorSeal{
			HolderID:       "c1",
			CertGeneration: 2,
			DutyMask:       rootstate.CoordinatorDutyMaskDefault,
			Frontiers:      controlplane.Frontiers(12, 34, 56),
		},
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	seal, err := sealLease(store, "c1", 200, controlplane.Frontiers(12, 34, 56))
	require.NoError(t, err)
	require.Equal(t, backend.seal, seal)
	require.Equal(t, 1, backend.leaseSealCalls)
}

func TestRootStoreConfirmCoordinatorClosureDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(127, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		closure: rootstate.CoordinatorClosure{
			HolderID:            "c1",
			SealGeneration:      2,
			SuccessorGeneration: 3,
			SealDigest:          "seal-digest",
			Stage:               rootstate.CoordinatorClosureStageConfirmed,
		},
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	closure, err := confirmClosure(store, "c1", 200)
	require.NoError(t, err)
	require.Equal(t, backend.closure, closure)
	require.Equal(t, 1, backend.leaseAuditCalls)
}

func TestRootStoreCloseCoordinatorClosureDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(127, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		closure: rootstate.CoordinatorClosure{
			HolderID:            "c1",
			SealGeneration:      2,
			SuccessorGeneration: 3,
			SealDigest:          "seal-digest",
			Stage:               rootstate.CoordinatorClosureStageClosed,
		},
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	closure, err := closeClosure(store, "c1", 200)
	require.NoError(t, err)
	require.Equal(t, backend.closure, closure)
	require.Equal(t, 1, backend.leaseCloseCalls)
}

func TestRootStoreReattachCoordinatorClosureDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(127, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		closure: rootstate.CoordinatorClosure{
			HolderID:            "c1",
			SealGeneration:      2,
			SuccessorGeneration: 3,
			SealDigest:          "seal-digest",
			Stage:               rootstate.CoordinatorClosureStageReattached,
		},
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	closure, err := reattachClosure(store, "c1", 200)
	require.NoError(t, err)
	require.Equal(t, backend.closure, closure)
	require.Equal(t, 1, backend.leaseReattachCalls)
}

func TestRootStoreClosePropagatesCloserError(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(122, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		closeErr: errors.New("close failed"),
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)
	require.ErrorIs(t, store.Close(), backend.closeErr)
}

type stubRootBackend struct {
	snapshot           rootstate.Snapshot
	observed           rootstorage.ObservedCommitted
	observeTailFn      func(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	waitForTailFn      func(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	tailNotifyCh       <-chan struct{}
	isLeaderValue      bool
	leaderIDValue      uint64
	closeErr           error
	closeCalls         int
	lease              rootstate.CoordinatorLease
	leaseCampaignErr   error
	leaseCampaignCalls int
	leaseReleaseErr    error
	leaseReleaseCalls  int
	seal               rootstate.CoordinatorSeal
	leaseSealErr       error
	leaseSealCalls     int
	closure            rootstate.CoordinatorClosure
	leaseAuditErr      error
	leaseAuditCalls    int
	leaseCloseErr      error
	leaseCloseCalls    int
	leaseReattachErr   error
	leaseReattachCalls int
}

func (s *stubRootBackend) Snapshot() (rootstate.Snapshot, error) {
	return rootstate.CloneSnapshot(s.snapshot), nil
}

func (*stubRootBackend) Append(...rootevent.Event) (rootstate.CommitInfo, error) {
	return rootstate.CommitInfo{}, nil
}

func (*stubRootBackend) FenceAllocator(rootstate.AllocatorKind, uint64) (uint64, error) {
	return 0, nil
}

func (s *stubRootBackend) ObserveCommitted() (rootstorage.ObservedCommitted, error) {
	return rootstorage.CloneObservedCommitted(s.observed), nil
}

func (s *stubRootBackend) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if s.observeTailFn == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return s.observeTailFn(after)
}

func (s *stubRootBackend) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if s.waitForTailFn == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return s.waitForTailFn(after, timeout)
}

func (s *stubRootBackend) TailNotify() <-chan struct{} {
	return s.tailNotifyCh
}

func (s *stubRootBackend) IsLeader() bool {
	return s.isLeaderValue
}

func (s *stubRootBackend) LeaderID() uint64 {
	return s.leaderIDValue
}

func (s *stubRootBackend) ApplyCoordinatorLease(cmd rootstate.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	switch cmd.Kind {
	case rootstate.CoordinatorLeaseCommandIssue:
		s.leaseCampaignCalls++
		if s.leaseCampaignErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.leaseCampaignErr
		}
	case rootstate.CoordinatorLeaseCommandRelease:
		s.leaseReleaseCalls++
		if s.leaseReleaseErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.leaseReleaseErr
		}
	}
	return rootstate.CoordinatorProtocolState{Lease: s.lease}, nil
}

func (s *stubRootBackend) ApplyCoordinatorClosure(cmd rootstate.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	switch cmd.Kind {
	case rootstate.CoordinatorClosureCommandSeal:
		s.leaseSealCalls++
		if s.leaseSealErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.leaseSealErr
		}
		return rootstate.CoordinatorProtocolState{Seal: s.seal}, nil
	case rootstate.CoordinatorClosureCommandConfirm:
		s.leaseAuditCalls++
		if s.leaseAuditErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.leaseAuditErr
		}
	case rootstate.CoordinatorClosureCommandClose:
		s.leaseCloseCalls++
		if s.leaseCloseErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.leaseCloseErr
		}
	case rootstate.CoordinatorClosureCommandReattach:
		s.leaseReattachCalls++
		if s.leaseReattachErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.leaseReattachErr
		}
	}
	return rootstate.CoordinatorProtocolState{Closure: s.closure}, nil
}

func (s *stubRootBackend) Close() error {
	s.closeCalls++
	return s.closeErr
}

func observedDescriptorsSnapshot(desc descriptor.Descriptor, cursor rootstate.Cursor) rootstorage.ObservedCommitted {
	return rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{
			Snapshot: rootstate.Snapshot{
				State:               rootstate.State{LastCommitted: cursor},
				Descriptors:         map[uint64]descriptor.Descriptor{desc.RegionID: desc},
				PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
				PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
			},
		},
	}
}

func observedDescriptorsTailSnapshot(desc descriptor.Descriptor, checkpointCursor, tailCursor rootstate.Cursor) rootstorage.ObservedCommitted {
	return rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{
			Snapshot: rootstate.Snapshot{
				State: rootstate.State{LastCommitted: checkpointCursor},
				Descriptors: map[uint64]descriptor.Descriptor{
					desc.RegionID: desc,
				},
				PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
				PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
			},
			TailOffset: 1,
		},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 1,
			StartOffset:     1,
			EndOffset:       2,
			Records: []rootstorage.CommittedEvent{
				{Cursor: tailCursor, Event: rootevent.RegionDescriptorPublished(desc)},
			},
		},
	}
}
