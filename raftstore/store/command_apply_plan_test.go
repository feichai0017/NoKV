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

	keys, barrier := commandApplyKeys(req)
	require.False(t, barrier)
	require.ElementsMatch(t, []string{"a", "b", "c", "d"}, keys)
}

func TestCommandApplyKeysTreatsRangeReadsAsBarrier(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_SCAN,
		Cmd:     &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("a")}},
	}}}

	keys, barrier := commandApplyKeys(req)
	require.True(t, barrier)
	require.Nil(t, keys)
}

func TestCommandApplyKeysTreatsUnknownResolveLockAsBarrier(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
		Cmd:     &raftcmdpb.Request_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockRequest{}},
	}}}

	keys, barrier := commandApplyKeys(req)
	require.True(t, barrier)
	require.Nil(t, keys)
}

func TestCommandApplyKeysDoesNotSerializeOnPrimaryLockOnly(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		testPrewriteRequestWithPrimary([]byte("primary"), []byte("secondary")),
	}}

	keys, barrier := commandApplyKeys(req)
	require.False(t, barrier)
	require.Equal(t, []string{"secondary"}, keys)
}

func testPrewriteRequest(key []byte) *raftcmdpb.Request {
	return testPrewriteRequestWithPrimary(key, key)
}

func testPrewriteRequestWithPrimary(primary, key []byte) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
		Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
			PrimaryLock: primary,
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
