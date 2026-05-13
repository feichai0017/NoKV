package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
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

func TestCommandApplyDependenciesTrackPerasCatalogInstall(t *testing.T) {
	segment, payload, digest := testCommandApplyPerasSegment(t)
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	installKeys, err := fsperas.PerasSegmentCatalogRouteInstallKeys(segment.Root, objectKey)
	require.NoError(t, err)
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{testPerasInstallSegmentRequest(segment, payload, digest, objectKey, false)}}

	deps, barrier := commandApplyDependencies(req)
	require.False(t, barrier)
	require.Contains(t, deps, testPerasSegment(segment.Root))
	for _, key := range installKeys {
		require.Contains(t, deps, testUserWriteBytes(key))
	}
}

func TestCommandApplyDependenciesCatalogInstallDoesNotDecodePayload(t *testing.T) {
	segment, _, digest := testCommandApplyPerasSegment(t)
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{testPerasInstallSegmentRequest(segment, []byte("not-a-segment-payload"), digest, objectKey, false)}}

	deps, barrier := commandApplyDependencies(req)
	require.False(t, barrier)
	require.Contains(t, deps, testPerasSegment(segment.Root))
	require.Contains(t, deps, testUserWriteBytes(objectKey))
}

func TestCommandApplyDependenciesTrackPerasMaterializeInstall(t *testing.T) {
	segment, payload, digest := testCommandApplyPerasSegment(t)
	firstKey, err := segment.FirstKey()
	require.NoError(t, err)
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{testPerasInstallSegmentRequest(segment, payload, digest, firstKey, true)}}

	deps, barrier := commandApplyDependencies(req)
	require.False(t, barrier)
	require.Contains(t, deps, testPerasSegment(segment.Root))
	require.Contains(t, deps, testUserWriteBytes(objectKey))
	for _, entry := range segment.EntriesView() {
		require.Contains(t, deps, testUserWriteBytes(entry.Key))
	}
}

func TestCommandApplyDependenciesTreatsInvalidPerasInstallAsBarrier(t *testing.T) {
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT,
		Cmd: &raftcmdpb.Request_PerasInstallSegment{PerasInstallSegment: &kvrpcpb.PerasInstallSegmentRequest{
			SegmentRoot:          make([]byte, 32),
			SegmentPayloadDigest: make([]byte, 32),
			SegmentPayload:       []byte("invalid"),
		}},
	}}}

	deps, barrier := commandApplyDependencies(req)
	require.True(t, barrier)
	require.Nil(t, deps)
}

func TestCommandApplyDependenciesTreatsInvalidPerasInstallRouteAsBarrier(t *testing.T) {
	segment, payload, digest := testCommandApplyPerasSegment(t)
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		testPerasInstallSegmentRequest(segment, payload, digest, []byte("not-a-segment-object"), false),
	}}

	deps, barrier := commandApplyDependencies(req)
	require.True(t, barrier)
	require.Nil(t, deps)
}

func testUserRead(key string) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyUserKey, hash: commandApplyDependencyHash([]byte(key))},
		mode: commandApplyDependencyRead,
	}
}

func testUserWriteBytes(key []byte) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyUserKey, hash: commandApplyDependencyHash(key)},
		mode: commandApplyDependencyWrite,
	}
}

func testUserWrite(key string) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyUserKey, hash: commandApplyDependencyHash([]byte(key))},
		mode: commandApplyDependencyWrite,
	}
}

func testPerasSegment(root [32]byte) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyPerasSegment, hash: commandApplyDependencyHash(root[:])},
		mode: commandApplyDependencyWrite,
	}
}

func testTxnIntent(key string, version uint64) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyTxnIntent, hash: commandApplyDependencyHash([]byte(key)), version: version},
		mode: commandApplyDependencyWrite,
	}
}

func testTxnPrimary(key string, version uint64) commandApplyDependency {
	return commandApplyDependency{
		key:  commandApplyDependencyKey{class: commandApplyDependencyTxnPrimary, hash: commandApplyDependencyHash([]byte(key)), version: version},
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

func testCommandApplyPerasSegment(t *testing.T) (fsperas.PerasSegment, []byte, [32]byte) {
	t.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 7)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind: fsmeta.OperationCreate,
			Mutations: []fsperas.ReplayMutation{
				{Key: dentryKey, Value: []byte("dentry")},
				{Key: inodeKey, Value: []byte("inode")},
			},
		}},
	})
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	return segment, payload, digest
}

func testPerasInstallSegmentRequest(segment fsperas.PerasSegment, payload []byte, digest [32]byte, routingKey []byte, materialize bool) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT,
		Cmd: &raftcmdpb.Request_PerasInstallSegment{PerasInstallSegment: &kvrpcpb.PerasInstallSegmentRequest{
			RoutingKey:           routingKey,
			SegmentRoot:          segment.Root[:],
			SegmentPayloadDigest: digest[:],
			SegmentPayload:       payload,
			InstallVersion:       1,
			MaterializeMvcc:      materialize,
		}},
	}
}

func BenchmarkCommandApplyDependenciesPerasCatalogInstall1000(b *testing.B) {
	segment, payload, digest := benchmarkCommandApplyPerasSegment(b, 1000)
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(b, err)
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{testPerasInstallSegmentRequest(segment, payload, digest, objectKey, false)}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deps, barrier := commandApplyDependencies(req)
		if barrier || len(deps) == 0 {
			b.Fatal("unexpected barrier")
		}
	}
}

func BenchmarkCommandApplyDependenciesPerasMaterializeInstall1000(b *testing.B) {
	segment, payload, digest := benchmarkCommandApplyPerasSegment(b, 1000)
	firstKey, err := segment.FirstKey()
	require.NoError(b, err)
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{testPerasInstallSegmentRequest(segment, payload, digest, firstKey, true)}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deps, barrier := commandApplyDependencies(req)
		if barrier || len(deps) == 0 {
			b.Fatal("unexpected barrier")
		}
	}
}

func benchmarkCommandApplyPerasSegment(b *testing.B, n int) (fsperas.PerasSegment, []byte, [32]byte) {
	b.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	mutations := make([]fsperas.ReplayMutation, 0, n)
	for i := range n {
		key, err := fsmeta.EncodeInodeKey(mount, fsmeta.InodeID(i+2))
		require.NoError(b, err)
		mutations = append(mutations, fsperas.ReplayMutation{Key: key, Value: []byte("inode")})
	}
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID:      fsperas.OperationID{ClientID: "bench", Seq: 1},
			Kind:      fsmeta.OperationUpdateInode,
			Mutations: mutations,
		}},
	})
	require.NoError(b, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(b, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(b, err)
	return segment, payload, digest
}
