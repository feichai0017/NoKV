package kv

import (
	"errors"
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/dbcore/commit"
	"github.com/feichai0017/NoKV/engine/index"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/feichai0017/NoKV/percolator/mvcc"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/utils"
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

	raftReq := &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("k1"), Version: 1}},
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

	cmdData, err := command.Encode(&raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("k1"), Version: 1}},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, applier([]myraft.Entry{{Type: myraft.EntryNormal, Data: cmdData}}))

	err = applier([]myraft.Entry{{Type: myraft.EntryNormal, Data: []byte("legacy")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported unframed raft payload")
}

func TestLockedErrorMapping(t *testing.T) {
	key := []byte("lock-key")
	require.Nil(t, lockedError(key, nil))

	lock := &mvcc.Lock{
		Primary:     []byte("primary"),
		Ts:          42,
		StartTime:   4200,
		TTL:         9000,
		Kind:        kvrpcpb.Mutation_Put,
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

func TestApplyMVCCMaintenanceAppliesInternalEntries(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
			Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
				Tombstones: []*kvrpcpb.InternalEntryTombstone{
					{
						ColumnFamily: kvrpcpb.InternalEntryTombstone_DEFAULT,
						Key:          []byte("maint-default"),
						Version:      11,
					},
					{
						ColumnFamily: kvrpcpb.InternalEntryTombstone_WRITE,
						Key:          []byte("maint-write"),
						Version:      22,
					},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	require.Equal(t, uint64(2), resp.GetResponses()[0].GetMvccMaintenance().GetAppliedEntries())
	require.Nil(t, resp.GetResponses()[0].GetMvccMaintenance().GetError())

	gotDefault, err := db.GetInternalEntry(entrykv.CFDefault, []byte("maint-default"), 11)
	require.NoError(t, err)
	defer gotDefault.DecrRef()
	require.NotZero(t, gotDefault.Meta&entrykv.BitDelete)

	gotWrite, err := db.GetInternalEntry(entrykv.CFWrite, []byte("maint-write"), 22)
	require.NoError(t, err)
	defer gotWrite.DecrRef()
	require.NotZero(t, gotWrite.Meta&entrykv.BitDelete)
}

func TestApplyTryAtomicMutateCommandMaterializesBothKeys(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.LSMShardCount = 1
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
			Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
				StartVersion:  10,
				CommitVersion: 11,
				Predicates: []*kvrpcpb.AtomicPredicate{
					{Key: []byte("dentry"), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
					{Key: []byte("inode"), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
				},
				Mutations: []*kvrpcpb.Mutation{
					{Op: kvrpcpb.Mutation_Put, Key: []byte("dentry"), Value: []byte("ino=42"), AssertionNotExist: true},
					{Op: kvrpcpb.Mutation_Put, Key: []byte("inode"), Value: []byte("attrs"), AssertionNotExist: true},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	require.Nil(t, resp.GetResponses()[0].GetTryAtomicMutate().GetError())
	require.False(t, resp.GetResponses()[0].GetTryAtomicMutate().GetFallbackToTwoPhaseCommit())
	require.Equal(t, uint64(2), resp.GetResponses()[0].GetTryAtomicMutate().GetAppliedKeys())

	reader := percolator.NewReader(db)
	dentry, _, err := reader.GetValue([]byte("dentry"), 11)
	require.NoError(t, err)
	require.Equal(t, []byte("ino=42"), dentry)
	inode, _, err := reader.GetValue([]byte("inode"), 11)
	require.NoError(t, err)
	require.Equal(t, []byte("attrs"), inode)
}

func TestApplyTryAtomicMutateCommandFallsBackForCrossShardBatch(t *testing.T) {
	const shardCount = 4

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.LSMShardCount = shardCount
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	startVersion := uint64(30)
	commitVersion := uint64(31)
	dentryKey, inodeKey := keysWithDifferentDefaultShardsForApplyTest(t, shardCount, startVersion)
	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
			Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
				StartVersion:  startVersion,
				CommitVersion: commitVersion,
				Predicates: []*kvrpcpb.AtomicPredicate{
					{Key: dentryKey, Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
					{Key: inodeKey, Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
				},
				Mutations: []*kvrpcpb.Mutation{
					{Op: kvrpcpb.Mutation_Put, Key: dentryKey, Value: []byte("ino=42"), AssertionNotExist: true},
					{Op: kvrpcpb.Mutation_Put, Key: inodeKey, Value: []byte("attrs"), AssertionNotExist: true},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	atomicResp := resp.GetResponses()[0].GetTryAtomicMutate()
	require.NotNil(t, atomicResp)
	require.Nil(t, atomicResp.GetError())
	require.True(t, atomicResp.GetFallbackToTwoPhaseCommit())
	require.Zero(t, atomicResp.GetAppliedKeys())

	reader := percolator.NewReader(db)
	for _, key := range [][]byte{dentryKey, inodeKey} {
		_, _, err := reader.GetValue(key, commitVersion)
		require.ErrorIs(t, err, utils.ErrKeyNotFound)
	}
}

func keysWithDifferentDefaultShardsForApplyTest(t *testing.T, shardCount int, version uint64) ([]byte, []byte) {
	t.Helper()
	keysByShard := make([][]byte, shardCount)
	for i := range 10000 {
		key := fmt.Appendf(nil, "apply-atomic-%d", i)
		shardID := commit.ShardForInternalKey(entrykv.InternalKey(entrykv.CFDefault, key, version), shardCount)
		if shardID >= 0 && shardID < shardCount && keysByShard[shardID] == nil {
			keysByShard[shardID] = key
		}
	}
	for low := range shardCount {
		for high := low + 1; high < shardCount; high++ {
			if keysByShard[low] != nil && keysByShard[high] != nil {
				return keysByShard[low], keysByShard[high]
			}
		}
	}
	t.Fatalf("could not find different-shard keys for shardCount=%d", shardCount)
	return nil, nil
}

func TestApplyMVCCMaintenanceRejectsMalformedBatch(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	resp, err := Apply(db, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
			Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
				Tombstones: []*kvrpcpb.InternalEntryTombstone{
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_ColumnFamily(99), Key: []byte("bad-cf"), Version: 1},
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_DEFAULT, Key: []byte("must-not-apply"), Version: 1},
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResponses(), 1)
	keyErr := resp.GetResponses()[0].GetMvccMaintenance().GetError()
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetAbort(), "column family")
	_, err = db.GetInternalEntry(entrykv.CFDefault, []byte("must-not-apply"), 1)
	require.Error(t, err)
}

func TestApplyMVCCMaintenancePropagatesStoreBatchError(t *testing.T) {
	storeErr := errors.New("store batch failed")
	store := &failingMaintenanceStore{err: storeErr}
	resp, err := Apply(store, nil, &raftcmdpb.RaftCmdRequest{
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
			Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
				Tombstones: []*kvrpcpb.InternalEntryTombstone{
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_DEFAULT, Key: []byte("a"), Version: 1},
					{ColumnFamily: kvrpcpb.InternalEntryTombstone_WRITE, Key: []byte("b"), Version: 2},
				},
			}},
		}},
	})
	require.ErrorIs(t, err, storeErr)
	require.Nil(t, resp)
	require.Equal(t, 1, store.calls)
	require.Equal(t, 2, store.entries)
}

type failingMaintenanceStore struct {
	err     error
	calls   int
	entries int
}

func (s *failingMaintenanceStore) ApplyInternalEntries(entries []*entrykv.Entry) error {
	s.calls++
	s.entries += len(entries)
	return s.err
}

func (s *failingMaintenanceStore) GetInternalEntry(entrykv.ColumnFamily, []byte, uint64) (*entrykv.Entry, error) {
	return nil, errors.New("not implemented")
}

func (s *failingMaintenanceStore) NewInternalIterator(*index.Options) index.Iterator {
	return nil
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
	write := mvcc.EncodeWrite(mvcc.Write{
		Kind:       kvrpcpb.Mutation_Put,
		StartTs:    startTs,
		ShortValue: []byte("short-v"),
		ExpiresAt:  expiresAt,
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, commitTs, write, 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
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

func TestHandleScanCommittedLockDoesNotHideVisiblePut(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("scan-lock-visible-put")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value1"), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 20, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: 10,
	}), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 30, nil, entrykv.BitDelete, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 40, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Lock,
		StartTs: 30,
	}), 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      50,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, key, resp.GetKvs()[0].GetKey())
	require.Equal(t, []byte("value1"), resp.GetKvs()[0].GetValue())
}

func TestHandleScanCommittedLockDoesNotCreateVisibleKey(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("scan-lock-only")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, nil, entrykv.BitDelete, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 20, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Lock,
		StartTs: 10,
	}), 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      30,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	require.Empty(t, resp.GetKvs())
}

func TestHandleScanRollbackMarkerDoesNotHideVisiblePut(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("scan-rollback-visible-put")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value1"), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 20, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: 10,
	}), 0, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 30, nil, entrykv.BitDelete, 0)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 30, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Rollback,
		StartTs: 30,
	}), 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      40,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, key, resp.GetKvs()[0].GetKey())
	require.Equal(t, []byte("value1"), resp.GetKvs()[0].GetValue())
}

func TestHandleScanSkipsExpiredShortValue(t *testing.T) {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("short-expired")
	write := mvcc.EncodeWrite(mvcc.Write{
		Kind:       kvrpcpb.Mutation_Put,
		StartTs:    11,
		ShortValue: []byte("short-v"),
		ExpiresAt:  1, // definitely expired
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 22, write, 0, 0)

	resp, err := handleScan(db, &kvrpcpb.ScanRequest{
		StartKey:     key,
		Limit:        1,
		Version:      30,
		IncludeStart: true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.GetKvs())
}
