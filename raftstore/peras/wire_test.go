package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/stretchr/testify/require"
)

func TestPerasWireRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: fsmeta.MountKeyID(9),
		Buckets:    []fsmeta.AffinityBucket{1, 3},
		Parents:    []fsmeta.InodeID{11, 12},
		Inodes:     []fsmeta.InodeID{21},
	}
	decodedScope, err := ScopeFromProto(ScopeToProto(scope))
	require.NoError(t, err)
	require.Equal(t, scope, decodedScope)

	prepare := wireTestPrepareRecord(t, scope)
	decodedPrepare, err := PrepareRecordFromProto(PrepareRecordToProto(prepare))
	require.NoError(t, err)
	require.Equal(t, prepare, decodedPrepare)

	prepareDigest, err := fsperas.PrepareDigest(prepare)
	require.NoError(t, err)
	commit := fsperas.CommitCertificateRecord{
		EpochID:           prepare.EpochID,
		OpID:              prepare.OpID,
		PrepareDigest:     prepareDigest,
		QuorumAckSet:      []string{"n1", "n2"},
		TimestampUnixNano: 5678,
		HolderID:          prepare.HolderID,
	}
	for i := range commit.HolderSignature {
		commit.HolderSignature[i] = byte(i)
	}
	decodedCommit, err := CommitCertificateRecordFromProto(CommitCertificateRecordToProto(commit))
	require.NoError(t, err)
	require.Equal(t, commit, decodedCommit)

	snapshot := fsperas.WitnessSnapshot{
		Prepares: []fsperas.PrepareRecord{prepare},
		Commits:  []fsperas.CommitCertificateRecord{commit},
	}
	decoded, err := SnapshotFromProto(SnapshotToProto(snapshot))
	require.NoError(t, err)
	require.Equal(t, snapshot, decoded)
}

func TestPerasWireRejectsWrongFixedDigestLength(t *testing.T) {
	prepare := PrepareRecordToProto(wireTestPrepareRecord(t, compile.AuthorityScope{Mount: "m1"}))
	prepare.DeltaDigest = prepare.DeltaDigest[:31]
	_, err := PrepareRecordFromProto(prepare)
	require.ErrorContains(t, err, "delta_digest length")
}

func wireTestPrepareRecord(t *testing.T, scope compile.AuthorityScope) fsperas.PrepareRecord {
	t.Helper()
	payload, err := fsperas.EncodeSemanticDeltaPayload(compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Authority:   scope,
		Eligibility: compile.EligibilityFastPath,
	})
	require.NoError(t, err)
	digest, err := fsperas.SemanticDeltaPayloadDigest(payload)
	require.NoError(t, err)
	record := fsperas.PrepareRecord{
		EpochID:            7,
		OpID:               fsperas.OperationID{ClientID: "client-a", Seq: 42},
		DeltaPayload:       payload,
		DeltaDigest:        digest,
		DependencyFrontier: []fsperas.OperationID{{ClientID: "client-a", Seq: 41}},
		TimestampUnixNano:  1234,
		HolderID:           "holder-a",
	}
	for i := range record.PredicateDigest {
		record.PredicateDigest[i] = byte(i)
	}
	for i := range record.AuthorityProofDigest {
		record.AuthorityProofDigest[i] = byte(i + 1)
	}
	for i := range record.HolderSignature {
		record.HolderSignature[i] = byte(i + 2)
	}
	_, err = fsperas.EncodePrepareRecord(record)
	require.NoError(t, err)
	return record
}
