package kv_test

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rscapsule "github.com/feichai0017/NoKV/raftstore/capsule"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type capsuleWitnessStub struct {
	prepareScope compile.AuthorityScope
	prepare      fscapsule.PrepareRecord
	commitScope  compile.AuthorityScope
	commit       fscapsule.CommitCertificateRecord
	probeEpoch   uint64
	snapshot     fscapsule.WitnessSnapshot
}

func (s *capsuleWitnessStub) AppendPrepare(_ context.Context, scope compile.AuthorityScope, record fscapsule.PrepareRecord) error {
	s.prepareScope = scope
	s.prepare = record
	return nil
}

func (s *capsuleWitnessStub) AppendCommitCertificate(_ context.Context, scope compile.AuthorityScope, record fscapsule.CommitCertificateRecord) error {
	s.commitScope = scope
	s.commit = record
	return nil
}

func (s *capsuleWitnessStub) Probe(_ context.Context, epochID uint64) (fscapsule.WitnessSnapshot, error) {
	s.probeEpoch = epochID
	return s.snapshot, nil
}

func TestServiceCapsuleWitnessPrepareCommitProbe(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 2,
		Buckets:    []fsmeta.AffinityBucket{4},
		Parents:    []fsmeta.InodeID{10},
		Inodes:     []fsmeta.InodeID{20},
	}
	prepare := serviceTestPrepareRecord(t, scope)
	prepareDigest, err := fscapsule.PrepareDigest(prepare)
	require.NoError(t, err)
	commit := fscapsule.CommitCertificateRecord{
		EpochID:           prepare.EpochID,
		OpID:              prepare.OpID,
		PrepareDigest:     prepareDigest,
		QuorumAckSet:      []string{"w1", "w2"},
		TimestampUnixNano: 30,
		HolderID:          prepare.HolderID,
	}
	witness := &capsuleWitnessStub{
		snapshot: fscapsule.WitnessSnapshot{
			Prepares: []fscapsule.PrepareRecord{prepare},
			Commits:  []fscapsule.CommitCertificateRecord{commit},
		},
	}
	service := kv.NewService(nil, kv.WithCapsuleWitness(witness))

	_, err = service.CapsuleWitnessPrepare(context.Background(), &kvrpcpb.CapsuleWitnessPrepareRequest{
		Scope:  rscapsule.ScopeToProto(scope),
		Record: rscapsule.PrepareRecordToProto(prepare),
	})
	require.NoError(t, err)
	require.Equal(t, scope, witness.prepareScope)
	require.Equal(t, prepare, witness.prepare)

	_, err = service.CapsuleWitnessCommit(context.Background(), &kvrpcpb.CapsuleWitnessCommitRequest{
		Scope:  rscapsule.ScopeToProto(scope),
		Record: rscapsule.CommitCertificateRecordToProto(commit),
	})
	require.NoError(t, err)
	require.Equal(t, scope, witness.commitScope)
	require.Equal(t, commit, witness.commit)

	resp, err := service.CapsuleWitnessProbe(context.Background(), &kvrpcpb.CapsuleWitnessProbeRequest{EpochId: prepare.EpochID})
	require.NoError(t, err)
	require.Equal(t, prepare.EpochID, witness.probeEpoch)
	decoded, err := rscapsule.SnapshotFromProto(resp)
	require.NoError(t, err)
	require.Equal(t, witness.snapshot, decoded)
}

func TestServiceCapsuleWitnessRequiresConfiguredNode(t *testing.T) {
	service := kv.NewService(nil)
	_, err := service.CapsuleWitnessProbe(context.Background(), &kvrpcpb.CapsuleWitnessProbeRequest{EpochId: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func serviceTestPrepareRecord(t *testing.T, scope compile.AuthorityScope) fscapsule.PrepareRecord {
	t.Helper()
	payload, err := fscapsule.EncodeSemanticDeltaPayload(compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Authority:   scope,
		Eligibility: compile.EligibilityFastPath,
	})
	require.NoError(t, err)
	digest, err := fscapsule.SemanticDeltaPayloadDigest(payload)
	require.NoError(t, err)
	record := fscapsule.PrepareRecord{
		EpochID:           3,
		OpID:              fscapsule.OperationID{ClientID: "client", Seq: 4},
		DeltaPayload:      payload,
		DeltaDigest:       digest,
		TimestampUnixNano: 20,
		HolderID:          "holder",
	}
	for i := range record.PredicateDigest {
		record.PredicateDigest[i] = 1
	}
	for i := range record.AuthorityProofDigest {
		record.AuthorityProofDigest[i] = 2
	}
	for i := range record.HolderSignature {
		record.HolderSignature[i] = 3
	}
	_, err = fscapsule.EncodePrepareRecord(record)
	require.NoError(t, err)
	return record
}
