package kv_test

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type perasWitnessStub struct {
	prepareScope compile.AuthorityScope
	prepare      fsperas.PrepareRecord
	commitScope  compile.AuthorityScope
	commit       fsperas.CommitCertificateRecord
	probeEpoch   uint64
	snapshot     fsperas.WitnessSnapshot
}

func (s *perasWitnessStub) AppendPrepare(_ context.Context, scope compile.AuthorityScope, record fsperas.PrepareRecord) error {
	s.prepareScope = scope
	s.prepare = record
	return nil
}

func (s *perasWitnessStub) AppendCommitCertificate(_ context.Context, scope compile.AuthorityScope, record fsperas.CommitCertificateRecord) error {
	s.commitScope = scope
	s.commit = record
	return nil
}

func (s *perasWitnessStub) Probe(_ context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	s.probeEpoch = epochID
	return s.snapshot, nil
}

func TestServicePerasWitnessPrepareCommitProbe(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 2,
		Buckets:    []fsmeta.AffinityBucket{4},
		Parents:    []fsmeta.InodeID{10},
		Inodes:     []fsmeta.InodeID{20},
	}
	prepare := serviceTestPrepareRecord(t, scope)
	prepareDigest, err := fsperas.PrepareDigest(prepare)
	require.NoError(t, err)
	commit := fsperas.CommitCertificateRecord{
		EpochID:           prepare.EpochID,
		OpID:              prepare.OpID,
		PrepareDigest:     prepareDigest,
		QuorumAckSet:      []string{"w1", "w2"},
		TimestampUnixNano: 30,
		HolderID:          prepare.HolderID,
	}
	witness := &perasWitnessStub{
		snapshot: fsperas.WitnessSnapshot{
			Prepares: []fsperas.PrepareRecord{prepare},
			Commits:  []fsperas.CommitCertificateRecord{commit},
		},
	}
	service := kv.NewService(nil, kv.WithPerasWitness(witness))

	_, err = service.PerasWitnessPrepare(context.Background(), &kvrpcpb.PerasWitnessPrepareRequest{
		Scope:  rsperas.ScopeToProto(scope),
		Record: rsperas.PrepareRecordToProto(prepare),
	})
	require.NoError(t, err)
	require.Equal(t, scope, witness.prepareScope)
	require.Equal(t, prepare, witness.prepare)

	_, err = service.PerasWitnessCommit(context.Background(), &kvrpcpb.PerasWitnessCommitRequest{
		Scope:  rsperas.ScopeToProto(scope),
		Record: rsperas.CommitCertificateRecordToProto(commit),
	})
	require.NoError(t, err)
	require.Equal(t, scope, witness.commitScope)
	require.Equal(t, commit, witness.commit)

	resp, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{EpochId: prepare.EpochID})
	require.NoError(t, err)
	require.Equal(t, prepare.EpochID, witness.probeEpoch)
	decoded, err := rsperas.SnapshotFromProto(resp)
	require.NoError(t, err)
	require.Equal(t, witness.snapshot, decoded)
}

func TestServicePerasWitnessRequiresConfiguredNode(t *testing.T) {
	service := kv.NewService(nil)
	_, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{EpochId: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func serviceTestPrepareRecord(t *testing.T, scope compile.AuthorityScope) fsperas.PrepareRecord {
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
		EpochID:           3,
		OpID:              fsperas.OperationID{ClientID: "client", Seq: 4},
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
	_, err = fsperas.EncodePrepareRecord(record)
	require.NoError(t, err)
	return record
}
