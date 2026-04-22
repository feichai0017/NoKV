package localmeta

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/vfs"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func withPendingRootEventLimit(t *testing.T, limit int, fn func()) {
	t.Helper()
	prev := maxPendingRootEvents
	maxPendingRootEvents = limit
	t.Cleanup(func() {
		maxPendingRootEvents = prev
	})
	fn()
}

func withBlockedRootEventLimit(t *testing.T, limit int, fn func()) {
	t.Helper()
	prev := maxBlockedRootEvents
	maxBlockedRootEvents = limit
	t.Cleanup(func() {
		maxBlockedRootEvents = prev
	})
	fn()
}

func TestLocalStorePersistsRegions(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	meta := RegionMeta{
		ID:       11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
	}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	got, ok := snapshot[meta.ID]
	require.True(t, ok)
	require.Equal(t, meta.ID, got.ID)
	require.Equal(t, meta.StartKey, got.StartKey)
	require.Equal(t, meta.EndKey, got.EndKey)
	require.FileExists(t, filepath.Join(dir, ReplicaStateFileName))
}

func TestLocalStoreDeleteRegion(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	meta := RegionMeta{ID: 7}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.DeleteRegion(meta.ID))
	_, ok := store.Snapshot()[meta.ID]
	require.False(t, ok)
}

func TestLocalStorePersistsRaftPointers(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	ptr := RaftLogPointer{
		GroupID:        7,
		Segment:        3,
		Offset:         2048,
		AppliedIndex:   42,
		AppliedTerm:    5,
		Committed:      41,
		SnapshotIndex:  64,
		SnapshotTerm:   7,
		TruncatedIndex: 11,
		TruncatedTerm:  2,
	}
	require.NoError(t, store.SaveRaftPointer(ptr))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	got, ok := reopened.RaftPointer(ptr.GroupID)
	require.True(t, ok)
	require.Equal(t, ptr, got)
	require.FileExists(t, filepath.Join(dir, RaftProgressFileName))
}

func TestLocalStorePersistsPendingRootEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	event := rootevent.PeerAdded(11, 2, 22, descriptor.Descriptor{
		RegionID: 11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
	})
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 7,
		Event:    event,
	}))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	pending := reopened.PendingRootEvents()
	require.Len(t, pending, 1)
	require.Equal(t, uint64(7), pending[0].Sequence)
	require.Equal(t, rootstate.TransitionIDFromEvent(event), rootstate.TransitionIDFromEvent(pending[0].Event))
	require.FileExists(t, filepath.Join(dir, PendingRootEventsFileName))
}

func TestLocalStoreDeletesPendingRootEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	event := rootevent.RegionTombstoned(9)
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 3,
		Event:    event,
	}))
	require.NoError(t, store.DeletePendingRootEvent(3))
	require.Empty(t, store.PendingRootEvents())
}

func TestLocalStoreRejectsPendingRootEventOverflow(t *testing.T) {
	withPendingRootEventLimit(t, 8, func() {
		dir := t.TempDir()
		store, err := OpenLocalStore(dir, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()

		for seq := uint64(1); seq <= uint64(maxPendingRootEvents); seq++ {
			require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
				Sequence: seq,
				Event:    rootevent.RegionTombstoned(seq),
			}))
		}
		err = store.SavePendingRootEvent(PendingRootEvent{
			Sequence: uint64(maxPendingRootEvents + 1),
			Event:    rootevent.RegionTombstoned(uint64(maxPendingRootEvents + 1)),
		})
		require.ErrorContains(t, err, "pending rooted event limit exceeded")
		require.Len(t, store.PendingRootEvents(), maxPendingRootEvents)
	})
}

func TestLocalStoreAllowsPendingRootEventOverwriteAtLimit(t *testing.T) {
	withPendingRootEventLimit(t, 8, func() {
		dir := t.TempDir()
		store, err := OpenLocalStore(dir, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()

		for seq := uint64(1); seq <= uint64(maxPendingRootEvents); seq++ {
			require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
				Sequence: seq,
				Event:    rootevent.RegionTombstoned(seq),
			}))
		}
		require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
			Sequence: uint64(maxPendingRootEvents),
			Event:    rootevent.StoreJoined(uint64(maxPendingRootEvents), "store-replaced"),
		}))
		pending := store.PendingRootEvents()
		require.Len(t, pending, maxPendingRootEvents)
		require.Equal(t, rootevent.KindStoreJoined, pending[len(pending)-1].Event.Kind)
	})
}

func TestLocalStorePersistsPendingSchedulerOperations(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	require.NoError(t, store.SavePendingSchedulerOperation(PendingSchedulerOperation{
		Kind:         PendingSchedulerOperationLeaderTransfer,
		RegionID:     17,
		SourcePeerID: 101,
		TargetPeerID: 202,
		Attempts:     3,
	}))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	ops := reopened.PendingSchedulerOperations()
	require.Len(t, ops, 1)
	require.Equal(t, PendingSchedulerOperationLeaderTransfer, ops[0].Kind)
	require.Equal(t, uint64(17), ops[0].RegionID)
	require.Equal(t, uint64(101), ops[0].SourcePeerID)
	require.Equal(t, uint64(202), ops[0].TargetPeerID)
	require.Equal(t, uint32(3), ops[0].Attempts)
	require.FileExists(t, filepath.Join(dir, PendingSchedulerOperationsFileName))
}

func TestLocalStoreMovesPendingRootEventToBlocked(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	event := rootevent.RegionTombstoned(23)
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 4,
		Event:    event,
	}))
	require.NoError(t, store.MovePendingRootEventToBlocked(4, BlockedRootEvent{
		Sequence:     4,
		Event:        event,
		TransitionID: rootstate.TransitionIDFromEvent(event),
		LastError:    "permanent reject",
	}))

	require.Empty(t, store.PendingRootEvents())
	blocked := store.BlockedRootEvents()
	require.Len(t, blocked, 1)
	require.Equal(t, uint64(4), blocked[0].Sequence)
	require.Equal(t, "permanent reject", blocked[0].LastError)
	require.Equal(t, rootstate.TransitionIDFromEvent(event), blocked[0].TransitionID)
	require.FileExists(t, filepath.Join(dir, BlockedRootEventsFileName))
}

func TestLocalStoreHelpersAndSnapshots(t *testing.T) {
	var nilStore *Store
	require.Empty(t, nilStore.WorkDir())
	require.Nil(t, nilStore.RaftPointerSnapshot())
	require.True(t, nilStore.Empty())

	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.Equal(t, dir, store.WorkDir())
	require.True(t, store.Empty())

	meta := RegionMeta{
		ID:       33,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 3},
	}
	require.NoError(t, store.SaveRegion(meta))
	require.False(t, store.Empty())

	snapshot := store.Snapshot()
	require.Len(t, snapshot, 1)
	snapshot[meta.ID] = RegionMeta{ID: meta.ID, StartKey: []byte("mutated")}
	require.Equal(t, []byte("a"), store.Snapshot()[meta.ID].StartKey)

	ptr := RaftLogPointer{GroupID: 9, Segment: 2, Offset: 128, AppliedIndex: 11}
	require.NoError(t, store.SaveRaftPointer(ptr))
	ptrs := store.RaftPointerSnapshot()
	require.Equal(t, ptr, ptrs[ptr.GroupID])
	ptrs[ptr.GroupID] = RaftLogPointer{}
	got, ok := store.RaftPointer(ptr.GroupID)
	require.True(t, ok)
	require.Equal(t, ptr, got)
}

func TestLocalStoreDeletePendingSchedulerOperation(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.NoError(t, store.DeletePendingSchedulerOperation(PendingSchedulerOperationUnknown, 0))

	op := PendingSchedulerOperation{
		Kind:         PendingSchedulerOperationLeaderTransfer,
		RegionID:     19,
		SourcePeerID: 301,
		TargetPeerID: 302,
	}
	require.NoError(t, store.SavePendingSchedulerOperation(op))
	require.Len(t, store.PendingSchedulerOperations(), 1)

	require.NoError(t, store.DeletePendingSchedulerOperation(op.Kind, op.RegionID))
	require.Empty(t, store.PendingSchedulerOperations())
}

func TestLoadBlockedRootEventCatalogLimit(t *testing.T) {
	withBlockedRootEventLimit(t, 1, func() {
		dir := t.TempDir()
		path := filepath.Join(dir, BlockedRootEventsFileName)
		catalog := &metapb.BlockedRootEventCatalog{
			Entries: []*metapb.BlockedRootEvent{
				{
					Sequence:     1,
					Event:        metawire.RootEventToProto(rootevent.RegionTombstoned(1)),
					TransitionId: "t1",
					LastError:    "stale",
				},
				{
					Sequence:     2,
					Event:        metawire.RootEventToProto(rootevent.RegionTombstoned(2)),
					TransitionId: "t2",
					LastError:    "stale",
				},
			},
		}
		payload, err := proto.Marshal(catalog)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, payload, 0o644))

		_, err = loadBlockedRootEventCatalog(vfs.OSFS{}, dir)
		require.ErrorContains(t, err, "blocked rooted event catalog exceeds limit")
	})
}

func TestPersistProtoFileRemovesTmpOnFileFailures(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		dir := t.TempDir()
		name := "persist-write.binpb"
		tmp := filepath.Join(dir, name) + ".tmp"
		injected := errors.New("write injected")
		fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
			vfs.FailOnceRule(vfs.OpFileWrite, tmp, injected),
		))

		err := persistProtoFile(fs, dir, name, []byte("payload"))
		require.ErrorIs(t, err, injected)
		_, statErr := os.Stat(tmp)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("sync", func(t *testing.T) {
		dir := t.TempDir()
		name := "persist-sync.binpb"
		tmp := filepath.Join(dir, name) + ".tmp"
		injected := errors.New("sync injected")
		fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
			vfs.FailOnceRule(vfs.OpFileSync, tmp, injected),
		))

		err := persistProtoFile(fs, dir, name, []byte("payload"))
		require.ErrorIs(t, err, injected)
		_, statErr := os.Stat(tmp)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("close", func(t *testing.T) {
		dir := t.TempDir()
		name := "persist-close.binpb"
		tmp := filepath.Join(dir, name) + ".tmp"
		injected := errors.New("close injected")
		fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
			vfs.FailOnceRule(vfs.OpFileClose, tmp, injected),
		))

		err := persistProtoFile(fs, dir, name, []byte("payload"))
		require.ErrorIs(t, err, injected)
		_, statErr := os.Stat(tmp)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})
}
