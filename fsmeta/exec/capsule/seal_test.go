package capsule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildCapsuleSealKeepsOnlyCommittedCertificates(t *testing.T) {
	firstPrepare := testSealPrepare()
	firstCommit := testCommitForPrepare(t, firstPrepare)
	prepareOnly := testSealPrepare()
	prepareOnly.OpID = OperationID{ClientID: "client-b", Seq: 1}

	seal, err := BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{firstPrepare, prepareOnly},
		Commits:  []CommitCertificateRecord{firstCommit},
	})
	require.NoError(t, err)

	require.Equal(t, uint64(1), seal.EpochID)
	require.Len(t, seal.Certificates, 1)
	require.Equal(t, firstPrepare.OpID, seal.Certificates[0].Prepare.OpID)
	require.NotZero(t, seal.DAGFrontierMerkle)
}

func TestBuildCapsuleSealOrdersByConflictDAG(t *testing.T) {
	firstPrepare := testSealPrepare()
	firstPrepare.OpID = OperationID{ClientID: "client-b", Seq: 2}
	secondPrepare := testSealPrepare()
	secondPrepare.OpID = OperationID{ClientID: "client-a", Seq: 1}
	secondPrepare.ConflictDAGFrontier = []OperationID{firstPrepare.OpID}

	seal, err := BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{secondPrepare, firstPrepare},
		Commits: []CommitCertificateRecord{
			testCommitForPrepare(t, secondPrepare),
			testCommitForPrepare(t, firstPrepare),
		},
	})
	require.NoError(t, err)

	require.Equal(t, []OperationID{firstPrepare.OpID, secondPrepare.OpID}, []OperationID{
		seal.Certificates[0].Prepare.OpID,
		seal.Certificates[1].Prepare.OpID,
	})
}

func TestBuildCapsuleSealRejectsMissingOrMismatchedPrepare(t *testing.T) {
	prepare := testSealPrepare()
	commit := testCommitForPrepare(t, prepare)

	_, err := BuildCapsuleSeal(1, WitnessSnapshot{Commits: []CommitCertificateRecord{commit}})
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)

	commit.PrepareDigest[0] ^= 0xff
	_, err = BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{prepare},
		Commits:  []CommitCertificateRecord{commit},
	})
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)
}

func TestBuildCapsuleSealRejectsMissingPredecessorAndCycles(t *testing.T) {
	firstPrepare := testSealPrepare()
	firstPrepare.ConflictDAGFrontier = []OperationID{{ClientID: "missing", Seq: 1}}
	_, err := BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{firstPrepare},
		Commits:  []CommitCertificateRecord{testCommitForPrepare(t, firstPrepare)},
	})
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)

	left := testSealPrepare()
	left.OpID = OperationID{ClientID: "left", Seq: 1}
	right := testSealPrepare()
	right.OpID = OperationID{ClientID: "right", Seq: 1}
	left.ConflictDAGFrontier = []OperationID{right.OpID}
	right.ConflictDAGFrontier = []OperationID{left.OpID}
	_, err = BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{left, right},
		Commits: []CommitCertificateRecord{
			testCommitForPrepare(t, left),
			testCommitForPrepare(t, right),
		},
	})
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)
}

func TestCapsuleSealMerkleStableAndSensitive(t *testing.T) {
	prepare := testSealPrepare()
	snapshot := WitnessSnapshot{
		Prepares: []PrepareRecord{prepare},
		Commits:  []CommitCertificateRecord{testCommitForPrepare(t, prepare)},
	}
	left, err := BuildCapsuleSeal(1, snapshot)
	require.NoError(t, err)
	right, err := BuildCapsuleSeal(1, snapshot)
	require.NoError(t, err)
	require.Equal(t, left.DAGFrontierMerkle, right.DAGFrontierMerkle)

	changedPayload := append(cloneBytes(snapshot.Prepares[0].DeltaPayload), 0xff)
	setPrepareDeltaPayload(&snapshot.Prepares[0], changedPayload)
	snapshot.Commits[0] = testCommitForPrepare(t, snapshot.Prepares[0])
	changed, err := BuildCapsuleSeal(1, snapshot)
	require.NoError(t, err)
	require.NotEqual(t, left.DAGFrontierMerkle, changed.DAGFrontierMerkle)
}

func BenchmarkBuildCapsuleSeal64(b *testing.B) {
	snapshot := sealSnapshotForBench(b, 64)

	b.ReportAllocs()
	for b.Loop() {
		seal, err := BuildCapsuleSeal(1, snapshot)
		if err != nil {
			b.Fatal(err)
		}
		if len(seal.Certificates) != 64 {
			b.Fatalf("unexpected cert count %d", len(seal.Certificates))
		}
	}
}

func testCommitForPrepare(t *testing.T, prepare PrepareRecord) CommitCertificateRecord {
	t.Helper()
	commit, err := commitForPrepare(prepare)
	require.NoError(t, err)
	return commit
}

func commitForPrepare(prepare PrepareRecord) (CommitCertificateRecord, error) {
	digest, err := PrepareDigest(prepare)
	if err != nil {
		return CommitCertificateRecord{}, err
	}
	commit := testCommitCertificateRecord(digest)
	commit.EpochID = prepare.EpochID
	commit.OpID = prepare.OpID
	commit.HolderID = prepare.HolderID
	return commit, nil
}

func sealSnapshotForBench(b *testing.B, n int) WitnessSnapshot {
	b.Helper()
	prepares := make([]PrepareRecord, 0, n)
	commits := make([]CommitCertificateRecord, 0, n)
	for i := range n {
		prepare := testSealPrepare()
		prepare.OpID = OperationID{ClientID: "bench", Seq: uint64(i + 1)}
		if i > 0 {
			prepare.ConflictDAGFrontier = []OperationID{{ClientID: "bench", Seq: uint64(i)}}
		}
		commit, err := commitForPrepare(prepare)
		if err != nil {
			b.Fatal(err)
		}
		prepares = append(prepares, prepare)
		commits = append(commits, commit)
	}
	return WitnessSnapshot{Prepares: prepares, Commits: commits}
}

func testSealPrepare() PrepareRecord {
	record := testPrepareRecord()
	record.ConflictDAGFrontier = nil
	return record
}
