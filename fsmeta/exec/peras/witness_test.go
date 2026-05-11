package peras

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestWitnessPrepareRecordCodecRoundTrip(t *testing.T) {
	record := testPrepareRecord()

	payload, err := EncodePrepareRecord(record)
	require.NoError(t, err)
	frame, err := DecodeWitnessFrame(payload)
	require.NoError(t, err)

	require.Equal(t, WitnessRecordPrepare, frame.Kind)
	require.Equal(t, record, frame.Prepare)
	payload[0] ^= 0xff
	require.Equal(t, testPrepareRecord(), frame.Prepare, "decode must not alias the payload")
}

func TestWitnessCommitCertificateCodecRoundTrip(t *testing.T) {
	prepare := testPrepareRecord()
	digest, err := PrepareDigest(prepare)
	require.NoError(t, err)
	record := testCommitCertificateRecord(digest)

	payload, err := EncodeCommitCertificateRecord(record)
	require.NoError(t, err)
	frame, err := DecodeWitnessFrame(payload)
	require.NoError(t, err)

	require.Equal(t, WitnessRecordCommitCertificate, frame.Kind)
	require.Equal(t, record, frame.Commit)
	payload[0] ^= 0xff
	require.Equal(t, testCommitCertificateRecord(digest), frame.Commit, "decode must not alias the payload")
}

func TestPrepareDigestStableAndSensitive(t *testing.T) {
	record := testPrepareRecord()
	left, err := PrepareDigest(record)
	require.NoError(t, err)
	right, err := PrepareDigest(record)
	require.NoError(t, err)
	require.Equal(t, left, right)

	nextPayload := append(cloneBytes(record.DeltaPayload), 0xff)
	setPrepareDeltaPayload(&record, nextPayload)
	changed, err := PrepareDigest(record)
	require.NoError(t, err)
	require.NotEqual(t, left, changed)
}

func TestWitnessCodecRejectsInvalidRecords(t *testing.T) {
	_, err := EncodePrepareRecord(PrepareRecord{EpochID: 1})
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)
	prepare := testPrepareRecord()
	prepare.DeltaDigest[0] ^= 0xff
	_, err = EncodePrepareRecord(prepare)
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)
	_, err = EncodeCommitCertificateRecord(CommitCertificateRecord{
		EpochID:  1,
		OpID:     OperationID{ClientID: "c", Seq: 1},
		HolderID: "holder-a",
	})
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)
	_, err = DecodeWitnessFrame([]byte("bad"))
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)
}

func TestWALWitnessLogAppendProbeAndReopen(t *testing.T) {
	dir := t.TempDir()
	manager, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	log, err := NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)

	prepare := testPrepareRecord()
	digest, err := PrepareDigest(prepare)
	require.NoError(t, err)
	commit := testCommitCertificateRecord(digest)
	other := prepare
	other.EpochID = 2
	other.OpID = OperationID{ClientID: "other", Seq: 1}

	prepareInfo, err := log.AppendPrepare(context.Background(), prepare)
	require.NoError(t, err)
	require.Equal(t, wal.RecordTypePerasWitness, prepareInfo.Type)
	commitInfo, err := log.AppendCommitCertificate(context.Background(), commit)
	require.NoError(t, err)
	require.Equal(t, wal.RecordTypePerasWitness, commitInfo.Type)
	_, err = log.AppendPrepare(context.Background(), other)
	require.NoError(t, err)
	require.NoError(t, manager.Close())

	reopened, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	reopenedLog, err := NewWALWitnessLog(reopened, wal.DurabilityFsync)
	require.NoError(t, err)

	snapshot, err := reopenedLog.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []PrepareRecord{prepare}, snapshot.Prepares)
	require.Equal(t, []CommitCertificateRecord{commit}, snapshot.Commits)

	snapshot, err = reopenedLog.Probe(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []PrepareRecord{other}, snapshot.Prepares)
	require.Empty(t, snapshot.Commits)
}

func BenchmarkEncodePrepareRecord(b *testing.B) {
	record := testPrepareRecord()

	b.ReportAllocs()
	for b.Loop() {
		payload, err := EncodePrepareRecord(record)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func testPrepareRecord() PrepareRecord {
	record := PrepareRecord{
		EpochID: 1,
		OpID: OperationID{
			ClientID: "client-a",
			Seq:      7,
		},
		DependencyFrontier: []OperationID{
			{ClientID: "client-a", Seq: 5},
			{ClientID: "client-b", Seq: 3},
		},
		TimestampUnixNano: 100,
		HolderID:          "holder-a",
	}
	setPrepareDeltaPayload(&record, testSemanticDeltaPayload())
	record.PredicateDigest[0] = 2
	record.AuthorityProofDigest[0] = 3
	record.HolderSignature[0] = 4
	return record
}

func setPrepareDeltaPayload(record *PrepareRecord, payload []byte) {
	record.DeltaPayload = cloneBytes(payload)
	digest, err := SemanticDeltaPayloadDigest(record.DeltaPayload)
	if err != nil {
		panic(err)
	}
	record.DeltaDigest = digest
}

func testSemanticDeltaPayload() []byte {
	payload, err := EncodeSemanticDeltaPayload(testSemanticDelta())
	if err != nil {
		panic(err)
	}
	return payload
}

func testSemanticDelta() compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind: fsmeta.OperationCreate,
		Plan: fsmeta.OperationPlan{
			Kind:       fsmeta.OperationCreate,
			Mount:      "vol",
			PrimaryKey: []byte("dentry/a"),
			ReadKeys: [][]byte{
				[]byte("dentry/a"),
			},
			MutateKeys: [][]byte{
				[]byte("dentry/a"),
				[]byte("inode/7"),
			},
		},
		Authority: compile.AuthorityScope{
			Mount:      "vol",
			MountKeyID: 1,
			Buckets:    []fsmeta.AffinityBucket{3},
			Parents:    []fsmeta.InodeID{fsmeta.RootInode},
			Inodes:     []fsmeta.InodeID{7},
		},
		ReadPredicates: []compile.Predicate{
			{Kind: compile.PredicateNotExists, Key: []byte("dentry/a")},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: []byte("dentry/a"), Value: []byte("inode=7")},
			{Kind: compile.EffectPut, Key: []byte("inode/7"), Value: []byte("attrs")},
		},
		RuntimeGuards: []compile.RuntimeGuard{compile.GuardQuotaCredit},
		Eligibility:   compile.EligibilityFastPath,
	}
}

func testCommitCertificateRecord(prepareDigest [32]byte) CommitCertificateRecord {
	record := CommitCertificateRecord{
		EpochID:           1,
		OpID:              OperationID{ClientID: "client-a", Seq: 7},
		PrepareDigest:     prepareDigest,
		QuorumAckSet:      []string{"store-1", "store-2"},
		TimestampUnixNano: 120,
		HolderID:          "holder-a",
	}
	record.HolderSignature[0] = 9
	return record
}
