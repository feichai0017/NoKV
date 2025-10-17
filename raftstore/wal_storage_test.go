package raftstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/wal"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestWalStorageReplayUpdatesManifest(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr := openManifestManager(t, dir)
	defer manifestMgr.Close()

	entries := []myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("foo")},
		{Index: 2, Term: 1, Data: []byte("bar")},
	}
	payload, err := encodeRaftEntries(1, entries)
	require.NoError(t, err)

	_, err = walMgr.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: payload,
	})
	require.NoError(t, err)
	require.NoError(t, walMgr.Sync())

	ws, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	lastIndex, err := ws.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), lastIndex)

	ptr, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(2), ptr.AppliedIndex)
	require.Equal(t, uint32(walMgr.ActiveSegment()), ptr.Segment)
}

func TestWalStorageSnapshotRecovery(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	ws, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	snap := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 10,
			Term:  3,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1},
			},
		},
		Data: []byte("snapshot-data"),
	}
	require.NoError(t, ws.ApplySnapshot(snap))
	require.NoError(t, walMgr.Sync())
	require.NoError(t, walMgr.Close())
	require.NoError(t, manifestMgr.Close())

	walMgr = openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr = openManifestManager(t, dir)
	defer manifestMgr.Close()

	wsReopen, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	recoveredSnap, err := wsReopen.Snapshot()
	require.NoError(t, err)
	require.False(t, myraft.IsEmptySnap(recoveredSnap))
	require.Equal(t, snap.Metadata.Index, recoveredSnap.Metadata.Index)
	require.Equal(t, snap.Metadata.Term, recoveredSnap.Metadata.Term)

	ptr, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, snap.Metadata.Index, ptr.SnapshotIndex)
	require.Equal(t, snap.Metadata.Term, ptr.SnapshotTerm)
	require.Equal(t, snap.Metadata.Index, ptr.AppliedIndex)
}

func TestWalStorageHardStateRecovery(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	ws, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	hs := myraft.HardState{
		Term:   4,
		Commit: 3,
		Vote:   2,
	}
	require.NoError(t, ws.SetHardState(hs))
	require.NoError(t, walMgr.Sync())
	require.NoError(t, walMgr.Close())
	require.NoError(t, manifestMgr.Close())

	walMgr = openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr = openManifestManager(t, dir)
	defer manifestMgr.Close()

	wsReopen, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	hsRecovered, _, err := wsReopen.InitialState()
	require.NoError(t, err)
	require.Equal(t, hs.Term, hsRecovered.Term)
	require.Equal(t, hs.Commit, hsRecovered.Commit)
	require.Equal(t, hs.Vote, hsRecovered.Vote)

	ptr, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, hs.Commit, ptr.Committed)
	require.Equal(t, hs.Term, ptr.AppliedTerm)
}

func TestWalStorageRecoversAfterManifestSkipEntry(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	ws, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	baseEntries := []myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("base-1")},
		{Index: 2, Term: 1, Data: []byte("base-2")},
	}
	require.NoError(t, ws.Append(baseEntries))
	lastBefore, err := ws.LastIndex()
	require.NoError(t, err)
	ptrBefore, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(2), ptrBefore.AppliedIndex)
	require.Equal(t, uint64(2), lastBefore)

	newEntries := []myraft.Entry{{Index: 3, Term: 2, Data: []byte("after-crash")}}
	payload, err := encodeRaftEntries(1, newEntries)
	require.NoError(t, err)
	_, err = walMgr.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: payload,
	})
	require.NoError(t, err)
	require.NoError(t, walMgr.Sync())

	ptrStill, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, ptrBefore, ptrStill)

	require.NoError(t, walMgr.Close())
	require.NoError(t, manifestMgr.Close())

	walMgr = openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr = openManifestManager(t, dir)
	defer manifestMgr.Close()

	wsReopen, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	lastAfter, err := wsReopen.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), lastAfter)

	ptrAfter, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Greater(t, ptrAfter.AppliedIndex, ptrBefore.AppliedIndex)
	require.Equal(t, uint64(3), ptrAfter.AppliedIndex)
}

func TestWalStorageRecoversAfterManifestSkipHardState(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	ws, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	initialState := myraft.HardState{Term: 2, Commit: 2, Vote: 1}
	require.NoError(t, ws.SetHardState(initialState))
	ptrBefore, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, initialState.Commit, ptrBefore.Committed)

	newState := myraft.HardState{Term: 3, Commit: 3, Vote: 2}
	payload, err := encodeRaftHardState(1, newState)
	require.NoError(t, err)
	_, err = walMgr.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftState,
		Payload: payload,
	})
	require.NoError(t, err)
	require.NoError(t, walMgr.Sync())

	ptrStill, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, ptrBefore, ptrStill)

	require.NoError(t, walMgr.Close())
	require.NoError(t, manifestMgr.Close())

	walMgr = openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr = openManifestManager(t, dir)
	defer manifestMgr.Close()

	wsReopen, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	recoveredState, _, err := wsReopen.InitialState()
	require.NoError(t, err)
	require.Equal(t, newState.Term, recoveredState.Term)
	require.Equal(t, newState.Commit, recoveredState.Commit)
	require.Equal(t, newState.Vote, recoveredState.Vote)

	ptrAfter, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, newState.Commit, ptrAfter.Committed)
	require.Equal(t, newState.Term, ptrAfter.AppliedTerm)
}

func TestWalStorageRecoversAfterManifestSkipEntryBatch(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	ws, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	initial := []myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("init-1")},
		{Index: 2, Term: 1, Data: []byte("init-2")},
	}
	require.NoError(t, ws.Append(initial))

	ptrBefore, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(2), ptrBefore.AppliedIndex)

	batchEntries := []myraft.Entry{
		{Index: 3, Term: 2, Data: []byte("batch-3")},
		{Index: 4, Term: 2, Data: []byte("batch-4")},
	}
	payload, err := encodeRaftEntries(1, batchEntries)
	require.NoError(t, err)
	_, err = walMgr.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: payload,
	})
	require.NoError(t, err)
	require.NoError(t, walMgr.Sync())

	ptrStale, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, ptrBefore.AppliedIndex, ptrStale.AppliedIndex)

	require.NoError(t, walMgr.Close())
	require.NoError(t, manifestMgr.Close())

	walMgr = openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr = openManifestManager(t, dir)
	defer manifestMgr.Close()

	wsReopen, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	lastIndex, err := wsReopen.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastIndex)

	ptrAfter, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(4), ptrAfter.AppliedIndex)
}

func TestWalStorageRecoversAfterManifestSkipSnapshot(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	_, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	snap := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 5,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1},
			},
		},
		Data: []byte("snapshot-before-crash"),
	}
	payload, err := encodeRaftSnapshot(1, snap)
	require.NoError(t, err)

	_, err = walMgr.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftSnapshot,
		Payload: payload,
	})
	require.NoError(t, err)
	require.NoError(t, walMgr.Sync())

	ptr, ok := manifestMgr.RaftPointer(1)
	if ok {
		require.Less(t, ptr.SnapshotIndex, snap.Metadata.Index)
	}

	require.NoError(t, walMgr.Close())
	require.NoError(t, manifestMgr.Close())

	walMgr = openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr = openManifestManager(t, dir)
	defer manifestMgr.Close()

	wsReopen, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	replayedSnap, err := wsReopen.Snapshot()
	require.NoError(t, err)
	require.False(t, myraft.IsEmptySnap(replayedSnap))
	require.Equal(t, snap.Metadata.Index, replayedSnap.Metadata.Index)
	require.Equal(t, snap.Metadata.Term, replayedSnap.Metadata.Term)

	ptrAfter, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, snap.Metadata.Index, ptrAfter.SnapshotIndex)
	require.Equal(t, snap.Metadata.Term, ptrAfter.SnapshotTerm)
}

func TestWalStorageRecoversMultipleGroupsAfterManifestSkip(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	wsGroup1, err := openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)
	wsGroup2, err := openWalStorage(WalStorageConfig{
		GroupID:  2,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	require.NoError(t, wsGroup1.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("g1-entry-1")},
	}))
	require.NoError(t, wsGroup2.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("g2-entry-1")},
	}))

	ptr1Before, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	ptr2Before, ok := manifestMgr.RaftPointer(2)
	require.True(t, ok)

	payloadGroup1, err := encodeRaftEntries(1, []myraft.Entry{
		{Index: 2, Term: 2, Data: []byte("g1-entry-2")},
	})
	require.NoError(t, err)
	payloadGroup2, err := encodeRaftEntries(2, []myraft.Entry{
		{Index: 2, Term: 2, Data: []byte("g2-entry-2")},
	})
	require.NoError(t, err)

	_, err = walMgr.AppendRecords(
		wal.Record{Type: wal.RecordTypeRaftEntry, Payload: payloadGroup1},
		wal.Record{Type: wal.RecordTypeRaftState, Payload: encodeHardStatePayload(t, 1, myraft.HardState{Term: 2, Commit: 2, Vote: 1})},
		wal.Record{Type: wal.RecordTypeRaftEntry, Payload: payloadGroup2},
	)
	require.NoError(t, err)
	require.NoError(t, walMgr.Sync())

	ptr1Stale, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, ptr1Before.AppliedIndex, ptr1Stale.AppliedIndex)
	ptr2Stale, ok := manifestMgr.RaftPointer(2)
	require.True(t, ok)
	require.Equal(t, ptr2Before.AppliedIndex, ptr2Stale.AppliedIndex)

	require.NoError(t, walMgr.Close())
	require.NoError(t, manifestMgr.Close())

	walMgr = openWalManager(t, dir)
	defer walMgr.Close()
	manifestMgr = openManifestManager(t, dir)
	defer manifestMgr.Close()

	wsGroup1, err = openWalStorage(WalStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)
	wsGroup2, err = openWalStorage(WalStorageConfig{
		GroupID:  2,
		WAL:      walMgr,
		Manifest: manifestMgr,
	})
	require.NoError(t, err)

	last1, err := wsGroup1.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last1)
	last2, err := wsGroup2.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last2)

	ptr1After, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(2), ptr1After.AppliedIndex)
	require.Equal(t, uint64(2), ptr1After.Committed)
	ptr2After, ok := manifestMgr.RaftPointer(2)
	require.True(t, ok)
	require.Equal(t, uint64(2), ptr2After.AppliedIndex)
}

func openWalManager(t *testing.T, dir string) *wal.Manager {
	t.Helper()
	mgr, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	return mgr
}

func openManifestManager(t *testing.T, dir string) *manifest.Manager {
	t.Helper()
	mgr, err := manifest.Open(dir)
	require.NoError(t, err)
	return mgr
}

func encodeHardStatePayload(t *testing.T, groupID uint64, st myraft.HardState) []byte {
	t.Helper()
	payload, err := encodeRaftHardState(groupID, st)
	require.NoError(t, err)
	return payload
}
