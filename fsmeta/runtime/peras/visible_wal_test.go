// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/stretchr/testify/require"
)

func TestWALVisibleLogReplaySkipsAppliedRecords(t *testing.T) {
	mgr, err := wal.Open(wal.Config{Dir: filepath.Join(t.TempDir(), "wal")})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})
	log, err := NewWALVisibleLog(mgr, wal.DurabilityFlushed)
	require.NoError(t, err)
	ctx := context.Background()
	first := testVisibleRecord(fsperas.OperationID{ClientID: "client", Seq: 9}, []byte("a"))
	second := testVisibleRecord(fsperas.OperationID{ClientID: "client", Seq: 10}, []byte("b"))

	require.NoError(t, log.AppendVisible(ctx, first))
	require.NoError(t, log.AppendVisible(ctx, second))
	require.NoError(t, log.AppendVisibleReplayPlanApplied(ctx, first.EpochID, first.HolderID, testVisibleReplayPlan(first)))
	require.Len(t, log.Records(), 1)

	replayed, err := log.ReplayVisible(ctx)
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.Equal(t, second.EpochID, replayed[0].EpochID)
	require.Equal(t, second.HolderID, replayed[0].HolderID)
	require.Equal(t, second.Scope.Mount, replayed[0].Scope.Mount)
	require.Equal(t, second.Scope.MountKeyID, replayed[0].Scope.MountKeyID)
	require.Equal(t, second.Scope.Parents, replayed[0].Scope.Parents)
	require.Equal(t, second.Operation, replayed[0].Operation)
}

func TestWALVisibleLogCompactsAppliedSegments(t *testing.T) {
	mgr, err := wal.Open(wal.Config{Dir: filepath.Join(t.TempDir(), "wal")})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})
	log, err := NewWALVisibleLog(mgr, wal.DurabilityFlushed)
	require.NoError(t, err)
	ctx := context.Background()
	record := testVisibleRecord(fsperas.OperationID{ClientID: "client", Seq: 9}, []byte("a"))

	require.NoError(t, log.AppendVisible(ctx, record))
	require.NoError(t, mgr.Rotate())
	replayed, err := log.ReplayVisible(ctx)
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.NoError(t, log.AppendVisibleReplayPlanApplied(ctx, record.EpochID, record.HolderID, testVisibleReplayPlan(record)))

	files, err := mgr.ListSegments()
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Contains(t, files[0], "00002.wal")
	replayed, err = log.ReplayVisible(ctx)
	require.NoError(t, err)
	require.Empty(t, replayed)
}

func TestWALVisibleLogAppliedRangesDoNotCoverGaps(t *testing.T) {
	mgr, err := wal.Open(wal.Config{Dir: filepath.Join(t.TempDir(), "wal")})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})
	log, err := NewWALVisibleLog(mgr, wal.DurabilityFlushed)
	require.NoError(t, err)
	ctx := context.Background()
	first := testVisibleRecord(fsperas.OperationID{ClientID: "client", Seq: 1}, []byte("a"))
	second := testVisibleRecord(fsperas.OperationID{ClientID: "client", Seq: 2}, []byte("b"))
	third := testVisibleRecord(fsperas.OperationID{ClientID: "client", Seq: 3}, []byte("c"))

	require.NoError(t, log.AppendVisible(ctx, first))
	require.NoError(t, log.AppendVisible(ctx, second))
	require.NoError(t, log.AppendVisible(ctx, third))
	require.NoError(t, log.AppendVisibleReplayPlanApplied(ctx, first.EpochID, first.HolderID, testVisibleReplayPlan(first, third)))

	replayed, err := log.ReplayVisible(ctx)
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.Equal(t, second.Operation.OpID, replayed[0].Operation.OpID)
}

func testVisibleReplayPlan(records ...fsperas.VisibleOperationRecord) fsperas.ReplayPlan {
	operations := make([]fsperas.ReplayOperation, 0, len(records))
	for _, record := range records {
		operations = append(operations, record.Operation)
	}
	return fsperas.ReplayPlan{
		EpochID:    7,
		Operations: operations,
	}
}

func testVisibleRecord(id fsperas.OperationID, key []byte) fsperas.VisibleOperationRecord {
	return fsperas.VisibleOperationRecord{
		EpochID:           7,
		HolderID:          "holder-a",
		GrantID:           "grant-a",
		GrantExpiresNanos: 123456789,
		RootLineage:       fsperas.VisibleRootLineage{ClusterEpoch: 1, Term: 2, Index: 3, Revision: 4},
		Scope: compile.AuthorityScope{
			Mount:      "m",
			MountKeyID: 1,
			Parents:    []fsmeta.InodeID{2},
		},
		Operation:         testVisibleReplayOperation(id, key),
		TimestampUnixNano: 1234,
	}
}

func testVisibleReplayOperation(id fsperas.OperationID, key []byte) fsperas.ReplayOperation {
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
		Digest:   testVisibleDigest(2),
	}
	return fsperas.ReplayOperation{
		OpID:                 id,
		Kind:                 fsmeta.OperationCreate,
		DescriptorDigest:     testVisibleDigest(1),
		PredicateProofDigest: compile.AdmissionProofSetDigest(nil, nil),
		ExecutionPlanDigest:  compile.ExecutionPlanDigest(segment, atomicity, compile.DurabilityVisibleOnly),
		Segment:              segment,
		Atomicity:            atomicity,
		Durability:           compile.DurabilityVisibleOnly,
		Mutations:            []fsperas.ReplayMutation{{Key: key, Value: []byte("value")}},
	}
}

func testVisibleDigest(seed byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = seed + byte(i)
	}
	return out
}
