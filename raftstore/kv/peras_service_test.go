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
	segmentScope compile.AuthorityScope
	segment      fsperas.SegmentWitnessRecord
	probeEpoch   uint64
	snapshot     fsperas.WitnessSnapshot
}

func (s *perasWitnessStub) AppendSegment(_ context.Context, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	s.segmentScope = scope
	s.segment = record
	return nil
}

func (s *perasWitnessStub) Probe(_ context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	s.probeEpoch = epochID
	return s.snapshot, nil
}

func TestServicePerasWitnessSegmentProbe(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 2,
		Buckets:    []fsmeta.AffinityBucket{4},
		Parents:    []fsmeta.InodeID{10},
		Inodes:     []fsmeta.InodeID{20},
	}
	record := serviceTestSegmentRecord()
	witness := &perasWitnessStub{
		snapshot: fsperas.WitnessSnapshot{Segments: []fsperas.SegmentWitnessRecord{record}},
	}
	service := kv.NewService(nil, kv.WithPerasWitness(witness))

	_, err := service.PerasWitnessSegment(context.Background(), &kvrpcpb.PerasWitnessSegmentRequest{
		Scope:  rsperas.ScopeToProto(scope),
		Record: rsperas.SegmentWitnessRecordToProto(record),
	})
	require.NoError(t, err)
	require.Equal(t, scope, witness.segmentScope)
	require.Equal(t, record, witness.segment)

	resp, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{EpochId: record.EpochID})
	require.NoError(t, err)
	require.Equal(t, record.EpochID, witness.probeEpoch)
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

func serviceTestSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 3
	var digest [32]byte
	digest[0] = 4
	return fsperas.SegmentWitnessRecord{
		EpochID:              3,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   2048,
		SegmentPointer:       "inline",
		OperationCount:       16,
		EntryCount:           32,
		TimestampUnixNano:    20,
		HolderID:             "holder",
	}
}
