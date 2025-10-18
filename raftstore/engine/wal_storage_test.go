package engine

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/wal"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestWALStorageSnapshotTracksTruncateSegment(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	ws, err := OpenWALStorage(WALStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
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

	ptr, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, snap.Metadata.Index, ptr.TruncatedIndex)
	require.Equal(t, snap.Metadata.Term, ptr.TruncatedTerm)
	require.Equal(t, uint32(2), ptr.Segment)
	require.Equal(t, uint64(1), ptr.SegmentIndex)
}

func TestWALStorageCompactUpdatesManifest(t *testing.T) {
	dir := t.TempDir()
	walMgr := openWalManager(t, dir)
	manifestMgr := openManifestManager(t, dir)

	ws, err := OpenWALStorage(WALStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
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

	ptr, ok := manifestMgr.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, uint64(3), ptr.TruncatedIndex)
	require.Equal(t, uint64(2), ptr.TruncatedTerm)
	require.Equal(t, uint64(ptr.Segment), ptr.SegmentIndex)

	_, ok = ws.segmentForIndex(2)
	require.False(t, ok)
	seg, ok := ws.segmentForIndex(4)
	require.True(t, ok)
	require.Equal(t, uint32(ptr.Segment), seg)
}

// Helpers duplicated from former package for test reuse.

func openWalManager(t *testing.T, dir string) *wal.Manager {
	t.Helper()
	mgr, err := wal.Open(wal.Config{Dir: filepath.Join(dir, "wal")})
	require.NoError(t, err)
	return mgr
}

func openManifestManager(t *testing.T, dir string) *manifest.Manager {
	t.Helper()
	mgr, err := manifest.Open(filepath.Join(dir, "manifest"))
	require.NoError(t, err)
	return mgr
}
