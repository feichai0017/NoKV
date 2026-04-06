package storage

import (
	"context"
	"errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
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
	updated := observedDescriptorsSnapshot(testDescriptor(92, []byte("m"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 2})
	backend := &stubRootBackend{observed: initial}
	backend.observeTailFn = func(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
		return updated.Advance(after, rootstorage.TailToken{Cursor: updated.LastCursor(), Revision: 1}), nil
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Contains(t, snapshot.Descriptors, uint64(91))

	advance, err := store.ObserveTail(rootstorage.TailToken{})
	require.NoError(t, err)
	require.True(t, advance.ShouldReloadState())

	snapshot, err = store.Load()
	require.NoError(t, err)
	require.NotContains(t, snapshot.Descriptors, uint64(91))
	require.Contains(t, snapshot.Descriptors, uint64(92))
}

func TestRootStoreWaitForTailRefreshesCachedSnapshot(t *testing.T) {
	initial := observedDescriptorsSnapshot(testDescriptor(101, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 1})
	updated := observedDescriptorsSnapshot(testDescriptor(102, []byte("m"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil), rootstate.Cursor{Term: 1, Index: 2})
	backend := &stubRootBackend{observed: initial}
	backend.waitForTailFn = func(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
		return updated.Advance(after, rootstorage.TailToken{Cursor: updated.LastCursor(), Revision: 1}), nil
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)

	advance, err := store.WaitForTail(rootstorage.TailToken{}, time.Second)
	require.NoError(t, err)
	require.True(t, advance.ShouldReloadState())

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Contains(t, snapshot.Descriptors, uint64(102))
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

func TestRootStoreCampaignDelegation(t *testing.T) {
	backend := &stubRootBackend{
		observed: observedDescriptorsSnapshot(
			testDescriptor(123, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			rootstate.Cursor{Term: 1, Index: 1},
		),
		campaignErr: errors.New("campaign failed"),
	}

	store, err := OpenRootStore(backend)
	require.NoError(t, err)
	require.ErrorIs(t, store.Campaign(), backend.campaignErr)
	require.Equal(t, 1, backend.campaignCalls)
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
	snapshot      rootstate.Snapshot
	observed      rootstorage.ObservedCommitted
	observeTailFn func(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	waitForTailFn func(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	tailNotifyCh  <-chan struct{}
	isLeaderValue bool
	leaderIDValue uint64
	closeErr      error
	closeCalls    int
	campaignErr   error
	campaignCalls int
}

func (s *stubRootBackend) Snapshot() (rootstate.Snapshot, error) {
	return rootstate.CloneSnapshot(s.snapshot), nil
}

func (*stubRootBackend) Append(...rootevent.Event) (rootstate.CommitInfo, error) {
	return rootstate.CommitInfo{}, nil
}

func (*stubRootBackend) FenceAllocator(rootpkg.AllocatorKind, uint64) (uint64, error) {
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

func (s *stubRootBackend) Campaign() error {
	s.campaignCalls++
	return s.campaignErr
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
