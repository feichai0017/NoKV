package kv

import (
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/stretchr/testify/require"
)

func applyVersionedEntryForApplyTest(t *testing.T, db *NoKV.DB, cf entrykv.ColumnFamily, key []byte, version uint64, value []byte, meta byte, expiresAt uint64) {
	t.Helper()
	entry := entrykv.NewInternalEntry(cf, key, version, entrykv.SafeCopy(nil, value), meta, expiresAt)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{entry}))
}

func TestNewEntryApplierAppliesEntries(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	applier := NewEntryApplier(db)

	raftReq := &pb.RaftCmdRequest{
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_GET,
			Cmd:     &pb.Request_Get{Get: &pb.GetRequest{Key: []byte("k1"), Version: 1}},
		}},
	}
	raftData, err := command.Encode(raftReq)
	require.NoError(t, err)

	err = applier([]myraft.Entry{
		{Type: myraft.EntryNormal, Data: raftData},
		{Type: myraft.EntryConfChange},
	})
	require.NoError(t, err)
}

func TestNewEntryApplierRejectsLegacyPayload(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	applier := NewEntryApplier(db)

	cmdData, err := command.Encode(&pb.RaftCmdRequest{
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_GET,
			Cmd:     &pb.Request_Get{Get: &pb.GetRequest{Key: []byte("k1"), Version: 1}},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, applier([]myraft.Entry{{Type: myraft.EntryNormal, Data: cmdData}}))

	err = applier([]myraft.Entry{{Type: myraft.EntryNormal, Data: []byte("legacy")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported legacy raft payload")
}

func TestLockedErrorMapping(t *testing.T) {
	key := []byte("lock-key")
	require.Nil(t, lockedError(key, nil))

	lock := &percolator.Lock{
		Primary:     []byte("primary"),
		Ts:          42,
		TTL:         9000,
		Kind:        pb.Mutation_Put,
		MinCommitTs: 100,
	}
	keyErr := lockedError(key, lock)
	require.NotNil(t, keyErr)
	require.NotNil(t, keyErr.GetLocked())
	require.Equal(t, lock.Primary, keyErr.GetLocked().GetPrimaryLock())
	require.Equal(t, entrykv.SafeCopy(nil, key), keyErr.GetLocked().GetKey())
	require.Equal(t, lock.Ts, keyErr.GetLocked().GetLockVersion())
	require.Equal(t, lock.TTL, keyErr.GetLocked().GetLockTtl())
	require.Equal(t, lock.Kind, keyErr.GetLocked().GetLockType())
	require.Equal(t, lock.MinCommitTs, keyErr.GetLocked().GetMinCommitTs())
}

func TestHandleScanShortValueCarriesExpiresAt(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("short-ttl")
	startTs := uint64(11)
	commitTs := uint64(22)
	expiresAt := ^uint64(0)
	write := percolator.EncodeWrite(percolator.Write{
		Kind:       pb.Mutation_Put,
		StartTs:    startTs,
		ShortValue: []byte("short-v"),
		ExpiresAt:  expiresAt,
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, commitTs, write, 0, 0)

	resp, err := handleScan(db, &pb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      30,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, key, resp.GetKvs()[0].GetKey())
	require.Equal(t, []byte("short-v"), resp.GetKvs()[0].GetValue())
	require.Equal(t, expiresAt, resp.GetKvs()[0].GetExpiresAt())
}

func TestHandleScanSkipsExpiredShortValue(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("short-expired")
	write := percolator.EncodeWrite(percolator.Write{
		Kind:       pb.Mutation_Put,
		StartTs:    11,
		ShortValue: []byte("short-v"),
		ExpiresAt:  1, // definitely expired
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 22, write, 0, 0)

	resp, err := handleScan(db, &pb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      30,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.GetKvs())
}
