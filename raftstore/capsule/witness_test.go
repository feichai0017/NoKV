package capsule

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/runtime/capsuleauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

var _ fscapsule.WitnessReplica = (*WitnessNode)(nil)

func TestWitnessNodeAppendPrepareCommitAndProbe(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	prepare := testPrepareRecord()
	commit := testCommitRecord(t, prepare)

	require.NoError(t, node.AppendPrepare(context.Background(), scope, prepare))
	require.NoError(t, node.AppendCommitCertificate(context.Background(), scope, commit))

	snapshot, err := node.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fscapsule.PrepareRecord{prepare}, snapshot.Prepares)
	require.Equal(t, []fscapsule.CommitCertificateRecord{commit}, snapshot.Commits)
}

func TestWitnessNodeRejectsMissingAuthority(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	require.NoError(t, node.authorities.Replace(nil))

	err := node.AppendPrepare(context.Background(), testAuthorityScope(), testPrepareRecord())
	require.ErrorIs(t, err, ErrWitnessAuthorityMissing)
}

func TestWitnessNodeRejectsWrongHolderAndEpoch(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()

	wrongHolder := testPrepareRecord()
	wrongHolder.HolderID = "holder-b"
	require.ErrorIs(t, node.AppendPrepare(context.Background(), scope, wrongHolder), ErrWitnessAuthorityMismatch)

	wrongEpoch := testPrepareRecord()
	wrongEpoch.EpochID = 2
	require.ErrorIs(t, node.AppendPrepare(context.Background(), scope, wrongEpoch), ErrWitnessAuthorityMismatch)
}

func TestWitnessNodeRejectsExpiredAuthority(t *testing.T) {
	now := time.Unix(100, 0)
	node, cleanup := openTestWitnessNodeWithNow(t, wal.DurabilityFsync, func() time.Time { return now })
	defer cleanup()
	expired := testAuthorityGrant()
	expired.ExpiresUnixNano = now.Add(-time.Nanosecond).UnixNano()
	require.NoError(t, node.authorities.Replace([]capsuleauth.AuthorityGrant{expired}))

	err := node.AppendPrepare(context.Background(), testAuthorityScope(), testPrepareRecord())
	require.ErrorIs(t, err, ErrWitnessAuthorityMissing)
}

func TestWitnessNodeRejectsCommitWithoutPrepare(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	prepare := testPrepareRecord()
	commit := testCommitRecord(t, prepare)

	err := node.AppendCommitCertificate(context.Background(), testAuthorityScope(), commit)
	require.ErrorIs(t, err, ErrWitnessPrepareMissing)
}

func TestWitnessNodeRejectsCommitWithMismatchedPrepareDigest(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	prepare := testPrepareRecord()
	commit := testCommitRecord(t, prepare)
	commit.PrepareDigest[0] ^= 0xff

	require.NoError(t, node.AppendPrepare(context.Background(), scope, prepare))
	err := node.AppendCommitCertificate(context.Background(), scope, commit)
	require.ErrorIs(t, err, ErrWitnessPrepareMismatch)
}

func TestWitnessNodeDuplicatePrepareIsIdempotentButConflictingRecordFails(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	prepare := testPrepareRecord()

	require.NoError(t, node.AppendPrepare(context.Background(), scope, prepare))
	require.NoError(t, node.AppendPrepare(context.Background(), scope, prepare))

	conflicting := prepare
	conflicting.DeltaPayload = append(append([]byte(nil), conflicting.DeltaPayload...), 0xff)
	digest, err := fscapsule.SemanticDeltaPayloadDigest(conflicting.DeltaPayload)
	require.NoError(t, err)
	conflicting.DeltaDigest = digest
	err = node.AppendPrepare(context.Background(), scope, conflicting)
	require.ErrorIs(t, err, ErrWitnessDuplicateRecord)
}

func TestWitnessNodeLoadsPrepareFromWALBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	log, err := fscapsule.NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)
	authorities := testActiveAuthorities(t, now)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: authorities,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	prepare := testPrepareRecord()
	commit := testCommitRecord(t, prepare)
	require.NoError(t, node.AppendPrepare(context.Background(), testAuthorityScope(), prepare))
	require.NoError(t, manager.Close())

	reopened, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	reopenedLog, err := fscapsule.NewWALWitnessLog(reopened, wal.DurabilityFsync)
	require.NoError(t, err)
	reopenedNode, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         reopenedLog,
		Authorities: authorities,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, reopenedNode.AppendCommitCertificate(context.Background(), testAuthorityScope(), commit))
	snapshot, err := reopenedNode.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fscapsule.PrepareRecord{prepare}, snapshot.Prepares)
	require.Equal(t, []fscapsule.CommitCertificateRecord{commit}, snapshot.Commits)
}

func TestWitnessNodeContextCancellationStopsAppend(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := node.AppendPrepare(ctx, testAuthorityScope(), testPrepareRecord())
	require.True(t, errors.Is(err, context.Canceled))
}

func BenchmarkWitnessNodeAppendPrepare(b *testing.B) {
	node, cleanup := openBenchWitnessNode(b)
	defer cleanup()
	scope := testAuthorityScope()
	record := testPrepareRecord()

	b.ReportAllocs()
	for b.Loop() {
		record.OpID.Seq++
		if err := node.AppendPrepare(context.Background(), scope, record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWitnessNodeAppendCommitCertificate(b *testing.B) {
	node, cleanup := openBenchWitnessNode(b)
	defer cleanup()
	scope := testAuthorityScope()
	prepare := testPrepareRecord()

	b.ReportAllocs()
	for b.Loop() {
		prepare.OpID.Seq++
		commit := testCommitRecord(b, prepare)
		if err := node.AppendPrepare(context.Background(), scope, prepare); err != nil {
			b.Fatal(err)
		}
		if err := node.AppendCommitCertificate(context.Background(), scope, commit); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHolderSubmitWithWitnessNodes(b *testing.B) {
	witnesses, cleanup := openBenchWitnessReplicas(b, 3)
	defer cleanup()
	holder, err := fscapsule.NewHolder(fscapsule.HolderConfig{
		EpochID:   1,
		HolderID:  "holder-a",
		Witnesses: witnesses,
		Now: func() time.Time {
			return time.Unix(100, 0)
		},
	})
	require.NoError(b, err)
	delta := compile.SemanticDelta{
		Authority:   testAuthorityScope(),
		Eligibility: compile.EligibilityFastPath,
		ReadPredicates: []compile.Predicate{
			{Kind: compile.PredicateNotExists, Key: []byte("bench-key")},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: []byte("bench-key"), Value: []byte("value")},
		},
	}

	b.ReportAllocs()
	var seq uint64
	for b.Loop() {
		seq++
		id := fscapsule.OperationID{ClientID: "bench", Seq: seq}
		commit, err := holder.Submit(context.Background(), id, delta)
		if err != nil {
			b.Fatal(err)
		}
		require.NoError(b, holder.MarkSealApplied(fscapsule.CapsuleSeal{
			EpochID: 1,
			Certificates: []fscapsule.SealedCertificate{{
				Prepare: fscapsule.PrepareRecord{
					EpochID: 1,
					OpID:    commit.OpID,
				},
			}},
		}))
	}
}

func openTestWitnessNode(t *testing.T, durability wal.DurabilityPolicy) (*WitnessNode, func()) {
	t.Helper()
	now := time.Unix(100, 0)
	return openTestWitnessNodeWithNow(t, durability, func() time.Time { return now })
}

func openTestWitnessNodeWithNow(t *testing.T, durability wal.DurabilityPolicy, now func() time.Time) (*WitnessNode, func()) {
	t.Helper()
	manager, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	log, err := fscapsule.NewWALWitnessLog(manager, durability)
	require.NoError(t, err)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: testActiveAuthorities(t, now()),
		Now:         now,
	})
	require.NoError(t, err)
	return node, func() { require.NoError(t, manager.Close()) }
}

func openBenchWitnessNode(b *testing.B) (*WitnessNode, func()) {
	b.Helper()
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: b.TempDir()})
	require.NoError(b, err)
	log, err := fscapsule.NewWALWitnessLog(manager, wal.DurabilityBuffered)
	require.NoError(b, err)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: testActiveAuthorities(b, now),
		Now:         func() time.Time { return now },
	})
	require.NoError(b, err)
	return node, func() { require.NoError(b, manager.Close()) }
}

func openBenchWitnessReplicas(b *testing.B, n int) ([]fscapsule.WitnessReplica, func()) {
	b.Helper()
	now := time.Unix(100, 0)
	table := testActiveAuthorities(b, now)
	witnesses := make([]fscapsule.WitnessReplica, 0, n)
	managers := make([]*wal.Manager, 0, n)
	for i := 0; i < n; i++ {
		manager, err := wal.Open(wal.Config{Dir: b.TempDir()})
		require.NoError(b, err)
		managers = append(managers, manager)
		log, err := fscapsule.NewWALWitnessLog(manager, wal.DurabilityBuffered)
		require.NoError(b, err)
		node, err := NewWitnessNode(WitnessNodeConfig{
			NodeID:      fmt.Sprintf("store-%d", i+1),
			Log:         log,
			Authorities: table,
			Now:         func() time.Time { return now },
		})
		require.NoError(b, err)
		witnesses = append(witnesses, node)
	}
	return witnesses, func() {
		for _, manager := range managers {
			require.NoError(b, manager.Close())
		}
	}
}

func testActiveAuthorities(tb testing.TB, now time.Time) *capsuleauth.ActiveAuthorities {
	tb.Helper()
	table := capsuleauth.NewActiveAuthorities()
	grant := testAuthorityGrant()
	grant.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	require.NoError(tb, table.Replace([]capsuleauth.AuthorityGrant{grant}))
	return table
}

func testAuthorityGrant() capsuleauth.AuthorityGrant {
	return rootproto.CapsuleAuthorityGrant{
		GrantID:         "grant-1",
		EpochID:         1,
		HolderID:        "holder-a",
		Scope:           capsuleauth.AuthorityScopeFromDelta(testAuthorityScope()),
		ExpiresUnixNano: time.Unix(101, 0).UnixNano(),
	}
}

func testAuthorityScope() compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      fsmeta.MountID("vol"),
		MountKeyID: fsmeta.MountKeyID(7),
		Buckets:    []fsmeta.AffinityBucket{3},
		Parents:    []fsmeta.InodeID{11},
		Inodes:     []fsmeta.InodeID{29},
	}
}

func testPrepareRecord() fscapsule.PrepareRecord {
	payload, err := fscapsule.EncodeSemanticDeltaPayload(testSemanticDelta())
	if err != nil {
		panic(err)
	}
	digest, err := fscapsule.SemanticDeltaPayloadDigest(payload)
	if err != nil {
		panic(err)
	}
	record := fscapsule.PrepareRecord{
		EpochID:             1,
		OpID:                fscapsule.OperationID{ClientID: "client-a", Seq: 1},
		DeltaPayload:        payload,
		DeltaDigest:         digest,
		ConflictDAGFrontier: []fscapsule.OperationID{},
		TimestampUnixNano:   100,
		HolderID:            "holder-a",
	}
	record.PredicateDigest[0] = 2
	record.AuthorityProofDigest[0] = 3
	record.HolderSignature[0] = 4
	return record
}

func testSemanticDelta() compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind: fsmeta.OperationCreate,
		Plan: fsmeta.OperationPlan{
			Kind:       fsmeta.OperationCreate,
			Mount:      "vol",
			PrimaryKey: []byte("dentry/a"),
			ReadKeys:   [][]byte{[]byte("dentry/a")},
			MutateKeys: [][]byte{[]byte("dentry/a"), []byte("inode/29")},
		},
		Authority: testAuthorityScope(),
		ReadPredicates: []compile.Predicate{
			{Kind: compile.PredicateNotExists, Key: []byte("dentry/a")},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: []byte("dentry/a"), Value: []byte("inode=29")},
			{Kind: compile.EffectPut, Key: []byte("inode/29"), Value: []byte("attrs")},
		},
		Eligibility: compile.EligibilityFastPath,
	}
}

func testCommitRecord(tb testing.TB, prepare fscapsule.PrepareRecord) fscapsule.CommitCertificateRecord {
	tb.Helper()
	digest, err := fscapsule.PrepareDigest(prepare)
	require.NoError(tb, err)
	record := fscapsule.CommitCertificateRecord{
		EpochID:           prepare.EpochID,
		OpID:              prepare.OpID,
		PrepareDigest:     digest,
		QuorumAckSet:      []string{"store-1", "store-2"},
		TimestampUnixNano: prepare.TimestampUnixNano + 1,
		HolderID:          prepare.HolderID,
	}
	record.HolderSignature[0] = 5
	return record
}
