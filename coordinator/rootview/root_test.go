package rootview

import (
	"context"
	"errors"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootclient "github.com/feichai0017/NoKV/meta/root/client"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

type fakeLoadStore struct {
	snapshot Snapshot
	err      error
}

func (f fakeLoadStore) Load() (Snapshot, error)                                  { return CloneSnapshot(f.snapshot), f.err }
func (f fakeLoadStore) AppendRootEvent(context.Context, rootevent.Event) error   { return nil }
func (f fakeLoadStore) SaveAllocatorState(context.Context, uint64, uint64) error { return nil }
func (f fakeLoadStore) ApplyTenure(context.Context, rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	return rootstate.SuccessionState{}, nil
}
func (f fakeLoadStore) ApplyHandover(context.Context, rootproto.HandoverCommand) (rootstate.SuccessionState, error) {
	return rootstate.SuccessionState{}, nil
}
func (f fakeLoadStore) Refresh() error   { return nil }
func (f fakeLoadStore) IsLeader() bool   { return true }
func (f fakeLoadStore) LeaderID() uint64 { return 1 }
func (f fakeLoadStore) Close() error     { return nil }

type fakeRootBackend struct {
	snapshot            rootstate.Snapshot
	observed            rootstorage.ObservedCommitted
	useObserved         bool
	tailAdvance         rootstorage.TailAdvance
	waitAdvance         rootstorage.TailAdvance
	appendErr           error
	fenceErr            error
	refreshErr          error
	observeErr          error
	waitErr             error
	observeCommittedErr error
	applyLeaseErr       error
	applyClosureErr     error
	snapshotErr         error
	refreshCount        int
	appendCalls         int
	fenceCalls          []rootstate.AllocatorKind
	closeCalled         bool
	isLeader            bool
	leaderID            uint64
	tailNotifyCh        chan struct{}
	applyLeaseResult    rootstate.SuccessionState
	applyClosureResult  rootstate.SuccessionState
}

func (f *fakeRootBackend) Snapshot() (rootstate.Snapshot, error) {
	if f.snapshotErr != nil {
		return rootstate.Snapshot{}, f.snapshotErr
	}
	return rootstate.CloneSnapshot(f.snapshot), nil
}

func (f *fakeRootBackend) Append(_ context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if f.appendErr != nil {
		return rootstate.CommitInfo{}, f.appendErr
	}
	f.appendCalls++
	for _, event := range events {
		cursor := rootstate.Cursor{
			Term:  max(f.snapshot.State.LastCommitted.Term, 1),
			Index: f.snapshot.State.LastCommitted.Index + 1,
		}
		rootstate.ApplyEventToSnapshot(&f.snapshot, cursor, event)
		if f.useObserved {
			f.observed.Checkpoint.Snapshot = rootstate.CloneSnapshot(f.snapshot)
			f.observed.Tail.Records = append(f.observed.Tail.Records, rootstorage.CommittedEvent{
				Cursor: cursor,
				Event:  rootevent.CloneEvent(event),
			})
			f.observed.Tail.EndOffset = int64(len(f.observed.Tail.Records))
		}
	}
	return rootstate.CommitInfo{Cursor: f.snapshot.State.LastCommitted, State: f.snapshot.State}, nil
}

func (f *fakeRootBackend) FenceAllocator(_ context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	if f.fenceErr != nil {
		return 0, f.fenceErr
	}
	f.fenceCalls = append(f.fenceCalls, kind)
	switch kind {
	case rootstate.AllocatorKindID:
		if min > f.snapshot.State.IDFence {
			f.snapshot.State.IDFence = min
		}
		if f.useObserved {
			f.observed.Checkpoint.Snapshot.State.IDFence = f.snapshot.State.IDFence
		}
		return f.snapshot.State.IDFence, nil
	case rootstate.AllocatorKindTSO:
		if min > f.snapshot.State.TSOFence {
			f.snapshot.State.TSOFence = min
		}
		if f.useObserved {
			f.observed.Checkpoint.Snapshot.State.TSOFence = f.snapshot.State.TSOFence
		}
		return f.snapshot.State.TSOFence, nil
	default:
		return 0, nil
	}
}

func (f *fakeRootBackend) Refresh() error {
	f.refreshCount++
	return f.refreshErr
}

func (f *fakeRootBackend) WaitForTail(rootstorage.TailToken, time.Duration) (rootstorage.TailAdvance, error) {
	if f.waitErr != nil {
		return rootstorage.TailAdvance{}, f.waitErr
	}
	return f.waitAdvance, nil
}

func (f *fakeRootBackend) ObserveTail(rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if f.observeErr != nil {
		return rootstorage.TailAdvance{}, f.observeErr
	}
	return f.tailAdvance, nil
}

func (f *fakeRootBackend) TailNotify() <-chan struct{} {
	return f.tailNotifyCh
}

func (f *fakeRootBackend) ObserveCommitted() (rootstorage.ObservedCommitted, error) {
	if f.observeCommittedErr != nil {
		return rootstorage.ObservedCommitted{}, f.observeCommittedErr
	}
	return rootstorage.CloneObservedCommitted(f.observed), nil
}

func (f *fakeRootBackend) IsLeader() bool   { return f.isLeader }
func (f *fakeRootBackend) LeaderID() uint64 { return f.leaderID }

func (f *fakeRootBackend) ApplyTenure(_ context.Context, _ rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	if f.applyLeaseErr != nil {
		return rootstate.SuccessionState{}, f.applyLeaseErr
	}
	f.snapshot.State.Tenure = f.applyLeaseResult.Tenure
	f.snapshot.State.Legacy = f.applyLeaseResult.Legacy
	f.snapshot.State.Handover = f.applyLeaseResult.Handover
	if f.useObserved {
		f.observed.Checkpoint.Snapshot = rootstate.CloneSnapshot(f.snapshot)
	}
	return f.applyLeaseResult, nil
}

func (f *fakeRootBackend) ApplyHandover(_ context.Context, _ rootproto.HandoverCommand) (rootstate.SuccessionState, error) {
	if f.applyClosureErr != nil {
		return rootstate.SuccessionState{}, f.applyClosureErr
	}
	f.snapshot.State.Tenure = f.applyClosureResult.Tenure
	f.snapshot.State.Legacy = f.applyClosureResult.Legacy
	f.snapshot.State.Handover = f.applyClosureResult.Handover
	if f.useObserved {
		f.observed.Checkpoint.Snapshot = rootstate.CloneSnapshot(f.snapshot)
	}
	return f.applyClosureResult, nil
}

func (f *fakeRootBackend) Close() error {
	f.closeCalled = true
	return nil
}

type fakeBasicRoot struct {
	snapshot rootstate.Snapshot
}

func (f fakeBasicRoot) Snapshot() (rootstate.Snapshot, error) {
	return rootstate.CloneSnapshot(f.snapshot), nil
}
func (f fakeBasicRoot) Append(context.Context, ...rootevent.Event) (rootstate.CommitInfo, error) {
	return rootstate.CommitInfo{}, nil
}
func (f fakeBasicRoot) FenceAllocator(context.Context, rootstate.AllocatorKind, uint64) (uint64, error) {
	return 0, nil
}

func TestSnapshotHelpersAndBootstrap(t *testing.T) {
	desc1 := testRootviewDescriptor(1, []byte("a"), []byte("m"))
	desc2 := testRootviewDescriptor(2, []byte("m"), []byte("z"))
	snapshot := Snapshot{
		ClusterEpoch: 5,
		RootToken:    rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 4}, Revision: 2},
		CatchUpState: CatchUpStateLagging,
		Descriptors: map[uint64]descriptor.Descriptor{
			desc2.RegionID: desc2,
			desc1.RegionID: desc1,
		},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
			desc1.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 9, PeerID: 19, Base: desc1, Target: desc2},
		},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
			desc1.RegionID: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: desc1.RegionID, Left: desc1, Right: desc2},
		},
		Allocator: AllocatorState{IDCurrent: 20, TSCurrent: 30},
		Tenure: rootstate.Tenure{
			HolderID:        "coord",
			Epoch:           7,
			ExpiresUnixNano: 999,
		},
	}

	cloned := CloneSnapshot(snapshot)
	cloned.Descriptors[desc1.RegionID].StartKey[0] = 'x'
	require.Equal(t, byte('a'), snapshot.Descriptors[desc1.RegionID].StartKey[0])

	rootSnapshot := rootstate.Snapshot{
		State: rootstate.State{
			ClusterEpoch:  5,
			LastCommitted: rootstate.Cursor{Term: 3, Index: 10},
			IDFence:       40,
			TSOFence:      50,
			Tenure:        snapshot.Tenure,
		},
		Descriptors:         snapshot.Descriptors,
		PendingPeerChanges:  snapshot.PendingPeerChanges,
		PendingRangeChanges: snapshot.PendingRangeChanges,
	}
	fromRoot := SnapshotFromRoot(rootSnapshot)
	require.Equal(t, CatchUpStateFresh, fromRoot.CatchUpState)
	require.Equal(t, uint64(40), fromRoot.Allocator.IDCurrent)
	require.Equal(t, rootstate.Cursor{Term: 3, Index: 10}, fromRoot.RootToken.Cursor)

	idStart, tsStart := ResolveAllocatorStarts(5, 6, AllocatorState{IDCurrent: 10, TSCurrent: 20})
	require.Equal(t, uint64(11), idStart)
	require.Equal(t, uint64(21), tsStart)
	idStart, tsStart = ResolveAllocatorStarts(100, 200, AllocatorState{IDCurrent: ^uint64(0), TSCurrent: ^uint64(0)})
	require.Equal(t, ^uint64(0), idStart)
	require.Equal(t, ^uint64(0), tsStart)

	var applied []uint64
	loaded, err := RestoreDescriptors(func(desc descriptor.Descriptor) error {
		applied = append(applied, desc.RegionID)
		if desc.RegionID == desc2.RegionID {
			return errors.New("stop")
		}
		return nil
	}, map[uint64]descriptor.Descriptor{
		0: desc1,
		2: desc2,
		1: desc1,
	})
	require.ErrorContains(t, err, "stop")
	require.Equal(t, 1, loaded)
	require.Equal(t, []uint64{1, 2}, applied)
	require.Equal(t, 0, mustRestoreDescriptorsNil(t))

	store := fakeLoadStore{snapshot: snapshot}
	info, err := Bootstrap(store, func(desc descriptor.Descriptor) error {
		applied = append(applied, desc.RegionID)
		return nil
	}, 5, 6)
	require.NoError(t, err)
	require.Equal(t, 2, info.LoadedRegions)
	require.Equal(t, uint64(21), info.IDStart)
	require.Equal(t, uint64(31), info.TSStart)
	require.Equal(t, snapshot, info.Snapshot)

	require.Equal(t, "fresh", CatchUpStateFresh.String())
	require.Equal(t, "bootstrap_required", CatchUpStateBootstrapRequired.String())
	require.Equal(t, "unspecified", CatchUpState(99).String())
}

func TestRemoteConfigAndNilStoreHelpers(t *testing.T) {
	require.ErrorContains(t, (RemoteRootConfig{}).Validate(), "requires at least one target")
	require.ErrorContains(t, (RemoteRootConfig{Targets: map[uint64]string{0: "127.0.0.1:1"}}).Validate(), "must be > 0")
	require.ErrorContains(t, (RemoteRootConfig{Targets: map[uint64]string{1: "   "}}).Validate(), "missing remote root address")
	require.NoError(t, (RemoteRootConfig{Targets: map[uint64]string{1: "127.0.0.1:1"}}).Validate())

	require.False(t, (&remoteRootBackend{}).IsLeader())
	require.True(t, (&remoteRootBackend{Client: &rootclient.Client{}}).IsLeader())

	var store *RootStore
	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Empty(t, snapshot.Descriptors)
	require.NoError(t, store.Refresh())
	require.Equal(t, rootstorage.TailAdvance{}, mustObserveTail(store))
	require.Equal(t, rootstorage.TailAdvance{}, mustWaitTail(store))
	require.Nil(t, store.SubscribeTail(rootstorage.TailToken{}))
	require.True(t, store.IsLeader())
	require.Zero(t, store.LeaderID())
	require.NoError(t, store.AppendRootEvent(context.Background(), rootevent.Event{}))
	require.NoError(t, store.SaveAllocatorState(context.Background(), 1, 2))
	leaseState, err := store.ApplyTenure(context.Background(), rootproto.TenureCommand{})
	require.NoError(t, err)
	require.Equal(t, rootstate.SuccessionState{}, leaseState)
	closureState, err := store.ApplyHandover(context.Background(), rootproto.HandoverCommand{})
	require.NoError(t, err)
	require.Equal(t, rootstate.SuccessionState{}, closureState)
	require.NoError(t, store.Close())
}

func TestRootStoreWithOptionalBackend(t *testing.T) {
	desc := testRootviewDescriptor(7, []byte("a"), []byte("m"))
	baseSnapshot := rootstate.Snapshot{
		State: rootstate.State{
			LastCommitted: rootstate.Cursor{Term: 1, Index: 1},
			IDFence:       10,
			TSOFence:      20,
		},
		Descriptors: map[uint64]descriptor.Descriptor{
			desc.RegionID: desc,
		},
	}
	initialObserved := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: baseSnapshot},
		Tail: rootstorage.CommittedTail{
			StartOffset: 1,
			EndOffset:   1,
		},
	}
	newDesc := testRootviewDescriptor(8, []byte("m"), []byte("z"))
	laggingObserved := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: baseSnapshot},
		Tail: rootstorage.CommittedTail{
			StartOffset: 1,
			EndOffset:   2,
			Records: []rootstorage.CommittedEvent{{
				Cursor: rootstate.Cursor{Term: 1, Index: 2},
				Event:  rootevent.RegionDescriptorPublished(newDesc),
			}},
		},
	}
	bootstrapObserved := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: baseSnapshot},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 0,
			StartOffset:     1,
			EndOffset:       2,
			Records: []rootstorage.CommittedEvent{{
				Cursor: rootstate.Cursor{Term: 1, Index: 2},
				Event:  rootevent.RegionDescriptorPublished(newDesc),
			}},
		},
	}

	fake := &fakeRootBackend{
		snapshot:     baseSnapshot,
		observed:     initialObserved,
		useObserved:  true,
		isLeader:     true,
		leaderID:     9,
		tailNotifyCh: make(chan struct{}),
		tailAdvance: laggingObserved.Advance(
			rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 1}, Revision: 1},
			rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Revision: 2},
		),
		waitAdvance: bootstrapObserved.Advance(
			rootstorage.TailToken{Cursor: rootstate.Cursor{}, Revision: 1},
			rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Revision: 2},
		),
		applyLeaseResult: rootstate.SuccessionState{
			Tenure: rootstate.Tenure{HolderID: "coord-2", Epoch: 2, ExpiresUnixNano: 999},
		},
		applyClosureResult: rootstate.SuccessionState{
			Handover: rootstate.Handover{
				HolderID:       "coord-2",
				LegacyEpoch:    2,
				SuccessorEpoch: 3,
				LegacyDigest:   "seal",
				Stage:          rootproto.HandoverStageClosed,
			},
		},
	}

	store, err := OpenRootStore(fake)
	require.NoError(t, err)
	loaded, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, desc, loaded.Descriptors[desc.RegionID])
	require.True(t, store.IsLeader())
	require.Equal(t, uint64(9), store.LeaderID())
	require.NotNil(t, store.SubscribeTail(rootstorage.TailToken{}))

	require.NoError(t, store.Refresh())
	require.Equal(t, 1, fake.refreshCount)

	advance, err := store.ObserveTail(rootstorage.TailToken{})
	require.NoError(t, err)
	require.Equal(t, CatchUpStateLagging, catchUpStateFromAdvance(advance))
	loaded, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, CatchUpStateLagging, loaded.CatchUpState)
	require.Equal(t, newDesc, loaded.Descriptors[newDesc.RegionID])

	advance, err = store.WaitForTail(rootstorage.TailToken{}, time.Second)
	require.NoError(t, err)
	require.Equal(t, CatchUpStateBootstrapRequired, catchUpStateFromAdvance(advance))
	loaded, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, CatchUpStateBootstrapRequired, loaded.CatchUpState)

	require.NoError(t, store.AppendRootEvent(context.Background(), rootevent.RegionTombstoned(desc.RegionID)))
	require.Equal(t, 1, fake.appendCalls)
	require.NoError(t, store.SaveAllocatorState(context.Background(), 55, 66))
	require.Equal(t, []rootstate.AllocatorKind{rootstate.AllocatorKindID, rootstate.AllocatorKindTSO}, fake.fenceCalls)

	leaseState, err := store.ApplyTenure(context.Background(), rootproto.TenureCommand{})
	require.NoError(t, err)
	require.Equal(t, "coord-2", leaseState.Tenure.HolderID)

	closureState, err := store.ApplyHandover(context.Background(), rootproto.HandoverCommand{})
	require.NoError(t, err)
	require.Equal(t, rootproto.HandoverStageClosed, closureState.Handover.Stage)

	require.NoError(t, store.Close())
	require.True(t, fake.closeCalled)
}

func TestRootStoreUnsupportedApplyCommands(t *testing.T) {
	store, err := OpenRootStore(fakeBasicRoot{
		snapshot: rootstate.Snapshot{Descriptors: map[uint64]descriptor.Descriptor{}},
	})
	require.NoError(t, err)

	_, err = store.ApplyTenure(context.Background(), rootproto.TenureCommand{})
	require.ErrorIs(t, err, errTenureCommandUnsupported)
	_, err = store.ApplyHandover(context.Background(), rootproto.HandoverCommand{})
	require.ErrorIs(t, err, errHandoverCommandUnsupported)
}

func mustRestoreDescriptorsNil(t *testing.T) int {
	t.Helper()
	loaded, err := RestoreDescriptors(nil, nil)
	require.NoError(t, err)
	return loaded
}

func mustObserveTail(store *RootStore) rootstorage.TailAdvance {
	advance, _ := store.ObserveTail(rootstorage.TailToken{})
	return advance
}

func mustWaitTail(store *RootStore) rootstorage.TailAdvance {
	advance, _ := store.WaitForTail(rootstorage.TailToken{}, time.Millisecond)
	return advance
}

func testRootviewDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: id*10 + 1}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
