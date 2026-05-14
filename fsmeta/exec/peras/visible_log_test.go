// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestVisibleOperationRecordRoundTrip(t *testing.T) {
	record := VisibleOperationRecord{
		EpochID:           7,
		HolderID:          "holder-a",
		GrantID:           "grant-a",
		GrantExpiresNanos: 123456789,
		RootLineage:       VisibleRootLineage{ClusterEpoch: 1, Term: 2, Index: 3, Revision: 4},
		Scope:             testVisibleAuthorityScope(),
		Operation:         testVisibleReplayOperation(OperationID{ClientID: "client", Seq: 9}, []byte("a")),
		TimestampUnixNano: 1234,
	}
	payload, err := EncodeVisibleOperationRecord(record)
	require.NoError(t, err)
	decoded, err := DecodeVisibleOperationRecord(payload)
	require.NoError(t, err)
	require.Equal(t, record, decoded)
}

func TestVisibleAppliedRecordRoundTrip(t *testing.T) {
	record := VisibleAppliedRecord{
		EpochID:  7,
		HolderID: "holder-a",
		Operations: []VisibleOperationReference{
			requireVisibleReference(t, testVisibleReplayOperation(OperationID{ClientID: "client", Seq: 9}, []byte("a"))),
			requireVisibleReference(t, testVisibleReplayOperation(OperationID{ClientID: "client", Seq: 10}, []byte("b"))),
		},
	}
	payload, err := EncodeVisibleAppliedRecord(record)
	require.NoError(t, err)
	decoded, err := DecodeVisibleAppliedRecord(payload)
	require.NoError(t, err)
	require.Equal(t, record, decoded)
}

func testVisibleReplayOperation(id OperationID, key []byte) ReplayOperation {
	segment := compile.SegmentPlan{
		MergeKey: compile.SegmentMergeKey{
			MountKeyID:    1,
			PrimaryBucket: 1,
			Install:       compile.SegmentInstallSingleBucket,
			Durability:    compile.DurabilityVisibleOnly,
			FormatVersion: 1,
		},
		Install:               compile.SegmentInstallSingleBucket,
		MaterializeMergeKey:   compile.SegmentMergeKey{MountKeyID: 1, PrimaryBucket: 1, Install: compile.SegmentInstallSingleBucket, Durability: compile.DurabilityVisibleOnly, FormatVersion: 1},
		MaterializeInstall:    compile.SegmentInstallSingleBucket,
		CanAppend:             true,
		CanMaterialize:        true,
		EstimatedPayloadBytes: 64,
		OperationCount:        1,
		MutationCount:         1,
	}
	atomicity := compile.AtomicityGroup{
		Members:  []compile.MutationID{1},
		Recovery: compile.RecoveryReplayAllOrNothing,
		Digest:   filledDigest(2),
	}
	return ReplayOperation{
		OpID:                 id,
		Kind:                 fsmeta.OperationCreate,
		DescriptorDigest:     filledDigest(1),
		PredicateProofDigest: compile.AdmissionProofSetDigest(nil, nil),
		ExecutionPlanDigest:  compile.ExecutionPlanDigest(segment, atomicity, compile.DurabilityVisibleOnly),
		Segment:              segment,
		Atomicity:            atomicity,
		Durability:           compile.DurabilityVisibleOnly,
		Mutations:            []ReplayMutation{{Key: key, Value: []byte("value")}},
	}
}

func requireVisibleReference(t *testing.T, op ReplayOperation) VisibleOperationReference {
	t.Helper()
	ref, err := VisibleOperationReferenceFromReplay(op)
	require.NoError(t, err)
	return ref
}

func testVisibleAuthorityScope() compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      "m",
		MountKeyID: 1,
		Buckets:    []fsmeta.AffinityBucket{},
		Parents:    []fsmeta.InodeID{2},
		Inodes:     []fsmeta.InodeID{},
	}
}

func filledDigest(seed byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = seed
	}
	return out
}
