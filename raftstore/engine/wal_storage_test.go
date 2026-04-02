package engine

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/kv"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestWALStorageSnapshotTracksTruncateSegment(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	localMeta := openLocalMetaStore(t, dir)

	ws, err := OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	require.NoError(t, ws.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("e1")},
		{Index: 2, Term: 1, Data: []byte("e2")},
		{Index: 3, Term: 1, Data: []byte("e3")},
		{Index: 4, Term: 2, Data: []byte("e4")},
	}))

	require.NoError(t, walMgr.SwitchSegment(2, true))

	require.NoError(t, ws.Append([]myraft.Entry{
		{Index: 5, Term: 2, Data: []byte("e5")},
		{Index: 6, Term: 2, Data: []byte("e6")},
	}))

	snap := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 4,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1},
			},
		},
		Data: []byte("snapshot-after-entries"),
	}
	require.NoError(t, ws.ApplySnapshot(snap))

	ptr, ok := localMeta.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, snap.Metadata.Index, ptr.TruncatedIndex)
	require.Equal(t, snap.Metadata.Term, ptr.TruncatedTerm)
	require.Equal(t, uint32(2), ptr.Segment)
	require.Equal(t, uint64(1), ptr.SegmentIndex)
	require.Greater(t, ptr.TruncatedOffset, uint64(0))
}

func TestWALStorageCompactUpdatesLocalMeta(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	localMeta := openLocalMetaStore(t, dir)

	ws, err := OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	require.NoError(t, ws.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("e1")},
		{Index: 2, Term: 1, Data: []byte("e2")},
	}))
	require.NoError(t, ws.Append([]myraft.Entry{
		{Index: 3, Term: 2, Data: []byte("e3")},
		{Index: 4, Term: 2, Data: []byte("e4")},
	}))

	require.NoError(t, ws.compactTo(3))

	ptr, ok := localMeta.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(3), ptr.TruncatedIndex)
	require.Equal(t, uint64(2), ptr.TruncatedTerm)
	require.Equal(t, uint64(ptr.Segment), ptr.SegmentIndex)
	require.Greater(t, ptr.TruncatedOffset, uint64(0))

	_, ok = ws.segmentForIndex(2)
	require.False(t, ok)
	seg, ok := ws.segmentForIndex(4)
	require.True(t, ok)
	require.Equal(t, uint32(ptr.Segment), seg)
}

func TestWALStorageRejectsLocalMetaPointerToNonRaftRecord(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	defer func() { _ = walMgr.Close() }()
	localMeta := openLocalMetaStore(t, dir)
	defer func() { _ = localMeta.Close() }()

	plain := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("plain"), 1), []byte("entry"))
	info, err := walMgr.AppendEntry(plain)
	require.NoError(t, err)
	plain.DecrRef()
	require.NoError(t, walMgr.Sync())

	ptr := localmeta.RaftLogPointer{
		GroupID: 1,
		Segment: info.SegmentID,
		Offset:  recordEnd(info),
	}
	require.NoError(t, localMeta.SaveRaftPointer(ptr))

	_, err = OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-raft record type")
}

func TestWALStorageDetectsTruncatedSegment(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	localMeta := openLocalMetaStore(t, dir)

	ws, err := OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	require.NoError(t, ws.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("entry-1")},
		{Index: 2, Term: 1, Data: []byte("entry-2")},
	}))
	require.NoError(t, walMgr.Sync())

	ptr, ok := localMeta.RaftPointer(1)
	require.True(t, ok)
	require.Greater(t, ptr.Offset, uint64(0))

	segmentPath := filepath.Join(dir, "wal", fmt.Sprintf("%05d.wal", ptr.Segment))

	require.NoError(t, localMeta.Close())
	require.NoError(t, walMgr.Close())

	file, err := os.OpenFile(segmentPath, os.O_WRONLY, 0)
	require.NoError(t, err)
	truncateTo := max(int64(ptr.Offset)-int64(walRecordOverhead), 0)
	require.NoError(t, file.Truncate(truncateTo))
	require.NoError(t, file.Close())

	walMgr = openWalManager(t, dir)
	defer func() { _ = walMgr.Close() }()
	localMeta = openLocalMetaStore(t, dir)
	defer func() { _ = localMeta.Close() }()

	_, err = OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "raft pointer")
}

func TestWALStorageValidatesLocalMetaPointerWithBacklog(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	localMeta := openLocalMetaStore(t, dir)

	ws1, err := OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)
	ws2, err := OpenWALStorage(WALStorageConfig{
		GroupID:   2,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	require.NoError(t, ws1.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("g1-1")},
		{Index: 2, Term: 1, Data: []byte("g1-2")},
		{Index: 3, Term: 2, Data: []byte("g1-3")},
	}))
	require.NoError(t, ws2.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("g2-1")},
	}))
	require.NoError(t, ws1.Append([]myraft.Entry{
		{Index: 4, Term: 2, Data: []byte("g1-4")},
		{Index: 5, Term: 3, Data: []byte("g1-5")},
	}))
	require.NoError(t, ws2.Append([]myraft.Entry{
		{Index: 2, Term: 2, Data: []byte("g2-2")},
		{Index: 3, Term: 3, Data: []byte("g2-3")},
	}))

	require.NoError(t, walMgr.Sync())
	require.NoError(t, localMeta.Close())
	require.NoError(t, walMgr.Close())

	walMgr = openWalManager(t, dir)
	defer func() { _ = walMgr.Close() }()
	localMeta = openLocalMetaStore(t, dir)
	defer func() { _ = localMeta.Close() }()

	ws1, err = OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	lastIdx, err := ws1.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIdx)

	ptr, ok := localMeta.RaftPointer(1)
	require.True(t, ok)
	require.NoError(t, validateRaftPointer(walMgr, ptr))
}

func TestWALStorageHardStateAndEntries(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	localMeta := openLocalMetaStore(t, dir)

	ws, err := OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	require.NoError(t, ws.SetHardState(myraft.HardState{}))
	hard := myraft.HardState{Term: 2, Vote: 5, Commit: 2}
	require.NoError(t, ws.SetHardState(hard))

	hs, _, err := ws.InitialState()
	require.NoError(t, err)
	require.Equal(t, hard, hs)

	require.NoError(t, ws.Append([]myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("e1")},
		{Index: 2, Term: 2, Data: []byte("e2")},
	}))

	first, err := ws.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	term, err := ws.Term(2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), term)

	entries, err := ws.Entries(1, 3, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	require.NoError(t, ws.MaybeCompact(2, 1))
}

func TestWALStorageEncodingHelpers(t *testing.T) {
	hs := myraft.HardState{Term: 3, Vote: 7, Commit: 9}
	data, err := encodeRaftHardState(5, hs)
	require.NoError(t, err)
	groupID, decoded, err := decodeRaftHardState(data)
	require.NoError(t, err)
	require.Equal(t, uint64(5), groupID)
	require.Equal(t, hs, decoded)

	snap := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 11, Term: 4},
		Data:     []byte("snap"),
	}
	snapData, err := encodeRaftSnapshot(8, snap)
	require.NoError(t, err)
	groupID, decodedSnap, err := decodeRaftSnapshot(snapData)
	require.NoError(t, err)
	require.Equal(t, uint64(8), groupID)
	require.Equal(t, snap.Metadata.Index, decodedSnap.Metadata.Index)
	require.Equal(t, snap.Metadata.Term, decodedSnap.Metadata.Term)
}

func TestWALSnapshotExportImport(t *testing.T) {
	baseDir := t.TempDir()
	walMgr := openWalManager(t, baseDir)
	localMeta := openLocalMetaStore(t, baseDir)

	ws, err := OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	sourceSnap := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      3,
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
		},
		Data: []byte("snapshot-state"),
	}
	require.NoError(t, ws.ApplySnapshot(sourceSnap))

	exportPath := filepath.Join(baseDir, "snapshot.bin")
	require.NoError(t, ExportSnapshot(ws, exportPath, nil))

	require.NoError(t, localMeta.Close())
	require.NoError(t, walMgr.Close())

	restoreDir := filepath.Join(baseDir, "restore")
	walMgrRestore := openWalManager(t, restoreDir)
	defer func() { _ = walMgrRestore.Close() }()
	localMetaRestore := openLocalMetaStore(t, restoreDir)
	defer func() { _ = localMetaRestore.Close() }()

	wsRestore, err := OpenWALStorage(WALStorageConfig{
		GroupID:   1,
		WAL:       walMgrRestore,
		LocalMeta: localMetaRestore,
	})
	require.NoError(t, err)

	require.NoError(t, ImportSnapshot(wsRestore, exportPath, nil))

	lastIdx, err := wsRestore.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIdx)

	ptr, ok := localMetaRestore.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(10), ptr.SnapshotIndex)
	require.Equal(t, uint64(3), ptr.SnapshotTerm)
}

// Helpers duplicated from former package for test reuse.

func openWalManager(t *testing.T, dir string) *wal.Manager {
	t.Helper()
	mgr, err := wal.Open(wal.Config{Dir: filepath.Join(dir, "wal")})
	require.NoError(t, err)
	return mgr
}

func openLocalMetaStore(t *testing.T, dir string) *localmeta.Store {
	t.Helper()
	mgr, err := localmeta.OpenLocalStore(filepath.Join(dir, "raftmeta"), nil)
	require.NoError(t, err)
	return mgr
}
