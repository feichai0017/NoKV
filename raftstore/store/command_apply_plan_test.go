package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
)

func TestCommandApplyKeysClassifiesPointWrites(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		testPrewriteRequest([]byte("a")),
		testCommitRequest([]byte("b")),
		testAtomicMutateRequest([]byte("c"), []byte("d")),
	}}

	deps, barrier := commandApplyDependencies(req)
	require.False(t, barrier)
	require.ElementsMatch(t, []commandApplyDependency{
		testUserWrite("a"),
		testTxnIntent("a", 0),
		testUserWrite("b"),
		testTxnIntent("b", 0),
		testUserRead("c"),
		testUserWrite("d"),
	}, deps)
}

func TestCommandApplyKeysTreatsRangeReadsAsBarrier(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_SCAN,
		Cmd:     &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("a")}},
	}}}

	keys, barrier := commandApplyDependencies(req)
	require.True(t, barrier)
	require.Nil(t, keys)
}

func TestCommandApplyKeysTreatsUnknownResolveLockAsBarrier(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
		Cmd:     &raftcmdpb.Request_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockRequest{}},
	}}}

	keys, barrier := commandApplyDependencies(req)
	require.True(t, barrier)
	require.Nil(t, keys)
}

func TestCommandApplyDependenciesTreatsMVCCMaintenanceAsBarrier(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
		Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
			Tombstones: []*kvrpcpb.InternalEntryTombstone{{
				Key:     []byte("k"),
				Version: 7,
			}},
		}},
	}}}

	deps, barrier := commandApplyDependencies(req)
	require.True(t, barrier)
	require.Nil(t, deps)
}

func TestCommandApplyKeysDoesNotSerializeOnPrimaryLockOnly(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		testPrewriteRequestWithPrimary([]byte("primary"), []byte("secondary")),
	}}

	deps, barrier := commandApplyDependencies(req)
	require.False(t, barrier)
	require.ElementsMatch(t, []commandApplyDependency{
		testUserWrite("secondary"),
		testTxnIntent("secondary", 0),
	}, deps)
}

func TestCommandApplyDependenciesTrackTxnPrimaryOperations(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{
			CmdType: raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS,
			Cmd: &raftcmdpb.Request_CheckTxnStatus{CheckTxnStatus: &kvrpcpb.CheckTxnStatusRequest{
				PrimaryKey: []byte("primary"),
				LockTs:     42,
			}},
		},
		{
			CmdType: raftcmdpb.CmdType_CMD_TXN_HEART_BEAT,
			Cmd: &raftcmdpb.Request_TxnHeartBeat{TxnHeartBeat: &kvrpcpb.TxnHeartBeatRequest{
				PrimaryKey:   []byte("primary"),
				StartVersion: 42,
			}},
		},
	}}

	deps, barrier := commandApplyDependencies(req)
	require.False(t, barrier)
	require.ElementsMatch(t, []commandApplyDependency{
		testUserWrite("primary"),
		testTxnPrimary("primary", 42),
		testUserWrite("primary"),
		testTxnPrimary("primary", 42),
	}, deps)
}

func testUserRead(key string) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyUserKey, key: key},
		mode: commandApplyDependencyRead,
	}
}

func testUserWrite(key string) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyUserKey, key: key},
		mode: commandApplyDependencyWrite,
	}
}

func testTxnIntent(key string, version uint64) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyTxnIntent, key: key, version: version},
		mode: commandApplyDependencyWrite,
	}
}

func testTxnPrimary(key string, version uint64) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyTxnPrimary, key: key, version: version},
		mode: commandApplyDependencyWrite,
	}
}

func testPrewriteRequest(key []byte) *raftcmdpb.Request {
	return testPrewriteRequestWithPrimary(key, key)
}

func testPrewriteRequestWithPrimary(primary, key []byte) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
		Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
			PrimaryLock:  primary,
			StartVersion: 0,
			Mutations: []*kvrpcpb.Mutation{{
				Op:  kvrpcpb.Mutation_Put,
				Key: key,
			}},
		}},
	}
}

func testCommitRequest(key []byte) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_COMMIT,
		Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
			Keys: [][]byte{key},
		}},
	}
}

func testAtomicMutateRequest(predicateKey, mutationKey []byte) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
		Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
			Predicates: []*kvrpcpb.AtomicPredicate{{Key: predicateKey}},
			Mutations: []*kvrpcpb.Mutation{{
				Op:  kvrpcpb.Mutation_Put,
				Key: mutationKey,
			}},
		}},
	}
}
