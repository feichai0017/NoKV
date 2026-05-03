package raftlog_test

import (
	"path/filepath"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	"github.com/stretchr/testify/require"
)

func TestDBLogRequiresOpenDB(t *testing.T) {
	var nilDB *NoKV.DB
	_, err := raftlog.NewDBLog(nilDB).Open(1, nil)
	require.ErrorContains(t, err, "db is required")

	db, localMeta := openDBLogTestDB(t)
	require.NoError(t, db.Close())
	_, err = raftlog.NewDBLog(db).Open(1, localMeta)
	require.ErrorContains(t, err, "closed db")
}

func TestDBLogUsesDedicatedControlWAL(t *testing.T) {
	db, localMeta := openDBLogTestDB(t)
	defer func() { _ = db.Close() }()

	storage, err := raftlog.NewDBLog(db).Open(9, localMeta)
	require.NoError(t, err)
	require.NoError(t, storage.Append([]myraft.Entry{{Index: 1, Term: 1, Data: []byte("raft")}}))

	for _, mgr := range db.LSMWALs() {
		require.Equal(t, uint64(0), mgr.Metrics().RecordCounts.RaftEntries)
	}
	matches, err := filepath.Glob(filepath.Join(db.WorkDir(), "control-wal-*", "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
}

func openDBLogTestDB(t *testing.T) (*NoKV.DB, *localmeta.Store) {
	t.Helper()
	dir := t.TempDir()
	localMeta, err := localmeta.OpenLocalStore(filepath.Join(dir, "raftmeta"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localMeta.Close() })

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.EnableWALWatchdog = false
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.RaftPointerSnapshot)
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db, localMeta
}
