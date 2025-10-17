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
