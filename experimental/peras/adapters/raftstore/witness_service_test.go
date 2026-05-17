// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore_test

import (
	"context"
	"testing"

	perasraftstore "github.com/feichai0017/NoKV/experimental/peras/adapters/raftstore"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type perasWitnessStub struct {
	segmentScope compile.AuthorityScope
	segment      fsperas.SegmentWitnessRecord
	segments     []fsperas.SegmentWitnessRecord
	probeEpoch   uint64
	snapshot     fsperas.WitnessSnapshot
	probeRef     fsperas.WitnessSegmentRef
}

type perasWitnessStatsStub struct {
	perasWitnessStub
	stats map[string]any
}

func (s *perasWitnessStatsStub) Stats() map[string]any {
	return s.stats
}

func (s *perasWitnessStub) AppendSegments(_ context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	s.segmentScope = scope
	s.segments = append(s.segments, records...)
	if len(records) > 0 {
		s.segment = records[len(records)-1]
	}
	return nil
}

func (s *perasWitnessStub) Probe(_ context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	s.probeEpoch = epochID
	return s.snapshot, nil
}

func (s *perasWitnessStub) ProbeSegment(_ context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	s.probeRef = ref
	for _, record := range s.snapshot.Segments {
		if record.EpochID == ref.EpochID && record.SegmentRoot == ref.SegmentRoot && record.SegmentPayloadDigest == ref.SegmentPayloadDigest {
			return record, true, nil
		}
	}
	return fsperas.SegmentWitnessRecord{}, false, nil
}

func TestServicePerasWitnessSegmentsSingleRecordProbe(t *testing.T) {
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
	service := perasraftstore.NewWitnessService(witness)

	_, err := service.PerasWitnessSegments(context.Background(), &kvrpcpb.PerasWitnessSegmentsRequest{
		Scope:   perasraftstore.ScopeToProto(scope),
		Records: []*kvrpcpb.PerasSegmentWitnessRecord{perasraftstore.SegmentWitnessRecordToProto(record)},
	})
	require.NoError(t, err)
	require.Equal(t, scope, witness.segmentScope)
	require.Equal(t, record, witness.segment)

	resp, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{EpochId: record.EpochID})
	require.NoError(t, err)
	require.Equal(t, record.EpochID, witness.probeEpoch)
	decoded, err := perasraftstore.SnapshotFromProto(resp)
	require.NoError(t, err)
	require.Equal(t, witness.snapshot, decoded)
}

func TestServicePerasWitnessSegments(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 2,
		Buckets:    []fsmeta.AffinityBucket{4},
		Parents:    []fsmeta.InodeID{10},
		Inodes:     []fsmeta.InodeID{20},
	}
	first := serviceTestSegmentRecordWithRoot(3)
	second := serviceTestSegmentRecordWithRoot(4)
	witness := &perasWitnessStub{}
	service := perasraftstore.NewWitnessService(witness)

	_, err := service.PerasWitnessSegments(context.Background(), &kvrpcpb.PerasWitnessSegmentsRequest{
		Scope: perasraftstore.ScopeToProto(scope),
		Records: []*kvrpcpb.PerasSegmentWitnessRecord{
			perasraftstore.SegmentWitnessRecordToProto(first),
			perasraftstore.SegmentWitnessRecordToProto(second),
		},
	})
	require.NoError(t, err)
	require.Equal(t, scope, witness.segmentScope)
	require.Equal(t, []fsperas.SegmentWitnessRecord{first, second}, witness.segments)
}

func TestServicePerasWitnessProbeSegment(t *testing.T) {
	first := serviceTestSegmentRecordWithRoot(3)
	second := serviceTestSegmentRecordWithRoot(4)
	witness := &perasWitnessStub{
		snapshot: fsperas.WitnessSnapshot{Segments: []fsperas.SegmentWitnessRecord{first, second}},
	}
	service := perasraftstore.NewWitnessService(witness)

	resp, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{
		EpochId:              second.EpochID,
		SegmentRoot:          append([]byte(nil), second.SegmentRoot[:]...),
		SegmentPayloadDigest: append([]byte(nil), second.SegmentPayloadDigest[:]...),
	})
	require.NoError(t, err)
	require.Equal(t, second.SegmentRoot, witness.probeRef.SegmentRoot)
	decoded, err := perasraftstore.SnapshotFromProto(resp)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{second}, decoded.Segments)
	require.Zero(t, witness.probeEpoch)
}

func TestServicePerasWitnessProbePagesSegments(t *testing.T) {
	first := serviceTestSegmentRecordWithRoot(1)
	second := serviceTestSegmentRecordWithRoot(2)
	third := serviceTestSegmentRecordWithRoot(3)
	witness := &perasWitnessStub{
		snapshot: fsperas.WitnessSnapshot{Segments: []fsperas.SegmentWitnessRecord{third, first, second}},
	}
	service := perasraftstore.NewWitnessService(witness)

	firstPage, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{
		EpochId: first.EpochID,
		Limit:   2,
	})
	require.NoError(t, err)
	require.True(t, firstPage.GetMore())
	require.Equal(t, first.SegmentRoot[:], firstPage.GetSegments()[0].GetSegmentRoot())
	require.Equal(t, second.SegmentRoot[:], firstPage.GetNextSegmentRoot())
	decoded, err := perasraftstore.SnapshotFromProto(firstPage)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{first, second}, decoded.Segments)

	secondPage, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{
		EpochId:                   first.EpochID,
		Limit:                     2,
		AfterSegmentRoot:          firstPage.GetNextSegmentRoot(),
		AfterSegmentPayloadDigest: firstPage.GetNextSegmentPayloadDigest(),
	})
	require.NoError(t, err)
	require.False(t, secondPage.GetMore())
	decoded, err = perasraftstore.SnapshotFromProto(secondPage)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{third}, decoded.Segments)
}

func TestServicePerasWitnessRequiresConfiguredNode(t *testing.T) {
	service := perasraftstore.NewWitnessService(nil)
	_, err := service.PerasWitnessProbe(context.Background(), &kvrpcpb.PerasWitnessProbeRequest{EpochId: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestServiceStatsIncludesPerasWitnessStats(t *testing.T) {
	witness := &perasWitnessStatsStub{stats: map[string]any{
		"append_total": uint64(3),
	}}
	service := perasraftstore.NewWitnessService(witness)

	stats := service.Stats()
	require.Equal(t, map[string]any{"append_total": uint64(3)}, stats)
}

func serviceTestSegmentRecord() fsperas.SegmentWitnessRecord {
	return serviceTestSegmentRecordWithRoot(3)
}

func serviceTestSegmentRecordWithRoot(rootByte byte) fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = rootByte
	var digest [32]byte
	digest[0] = rootByte + 1
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
