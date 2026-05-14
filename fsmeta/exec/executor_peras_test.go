// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecutorPerasPredicateReadsOverlayBeforeTimestamp(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", fsmeta.RootInode, "file")
	value := dentryValueForTest(t, fsmeta.RootInode, "file", 21, fsmeta.InodeTypeFile)
	committer := scanOverlayCommitter{
		values: overlayMapForTest(overlayValueForTest(key, value)),
	}
	executor, err := newTestExecutor(runner, WithPerasCommitter(committer))
	require.NoError(t, err)

	_, ok, err := executor.perasPredicatesHold(context.Background(), compile.MaterializedOp{CompiledOp: compile.CompiledOp{Delta: compile.SemanticDelta{
		ReadPredicates: []compile.Predicate{{
			Kind:             compile.PredicateObservedValue,
			Key:              key,
			ExpectedValue:    value,
			HasExpectedValue: true,
		}},
	}}}, fsperas.AdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), runner.nextTS, "overlay predicate admission must not reserve a read timestamp")
}

func TestExecutorPerasObservedPredicateRechecksExpectedValue(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", fsmeta.RootInode, "file")
	oldValue := dentryValueForTest(t, fsmeta.RootInode, "file", 21, fsmeta.InodeTypeFile)
	newValue := dentryValueForTest(t, fsmeta.RootInode, "file", 22, fsmeta.InodeTypeFile)
	runner.data[string(key)] = newValue
	committer := newTestPerasCommitter(t, runner)
	committer.RememberKey(key, true)
	executor, err := newTestExecutor(runner, WithPerasCommitter(committer))
	require.NoError(t, err)

	_, ok, err := executor.perasPredicatesHold(context.Background(), compile.MaterializedOp{CompiledOp: compile.CompiledOp{Delta: compile.SemanticDelta{
		ReadPredicates: []compile.Predicate{{
			Kind:             compile.PredicateObservedValue,
			Key:              key,
			ExpectedValue:    oldValue,
			HasExpectedValue: true,
			RuntimeChecked:   true,
		}},
	}}}, fsperas.AdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, runner.getCalls, "known-present facts cannot replace byte-level observed-value recheck")
}

func TestExecutorPerasPredicateRejectsCorruptProof(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", fsmeta.RootInode, "file")
	value := dentryValueForTest(t, fsmeta.RootInode, "file", 21, fsmeta.InodeTypeFile)
	runner.data[string(key)] = value
	executor, err := newTestExecutor(runner, WithPerasCommitter(newTestPerasCommitter(t, runner)))
	require.NoError(t, err)

	predicateProof := proof.NewPredicateProof(key, value, true, 7, proof.ReadSourceBase, proof.ProofFrontier{})
	predicateProof.Digest[0] ^= 0xff
	_, ok, err := executor.perasPredicatesHold(context.Background(), compile.MaterializedOp{
		CompiledOp: compile.CompiledOp{Delta: compile.SemanticDelta{
			ReadPredicates: []compile.Predicate{{
				Kind:             compile.PredicateObservedValue,
				Key:              key,
				ExpectedValue:    value,
				HasExpectedValue: true,
			}},
		}},
		PredicateProofs: []proof.PredicateProof{predicateProof},
	}, fsperas.AdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})

	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, runner.getCalls)
}

func TestExecutorMergePerasOverlayScanUsesOrderedMerge(t *testing.T) {
	executor := &Executor{perasCommitter: scanOverlayCommitter{rows: []fsperas.OverlayKV{
		{Key: []byte("k/b"), Delete: true},
		{Key: []byte("k/c"), Value: []byte("overlay-c")},
		{Key: []byte("k/e"), Value: []byte("overlay-e")},
	}}}
	base := []KV{
		{Key: []byte("k/a"), Value: []byte("base-a")},
		{Key: []byte("k/b"), Value: []byte("base-b")},
		{Key: []byte("k/d"), Value: []byte("base-d")},
	}

	merged := executor.mergePerasOverlayScan(base, []byte("k/"), 4)

	require.Equal(t, []KV{
		{Key: []byte("k/a"), Value: []byte("base-a")},
		{Key: []byte("k/c"), Value: []byte("overlay-c")},
		{Key: []byte("k/d"), Value: []byte("base-d")},
		{Key: []byte("k/e"), Value: []byte("overlay-e")},
	}, merged)
}

func BenchmarkExecutorAdmitPerasAuthorityOwned(b *testing.B) {
	executor, err := New(newFakeRunner(), WithPerasAuthorityAdmitter(ownedPerasAdmitter{}))
	if err != nil {
		b.Fatal(err)
	}
	delta := compile.SemanticDelta{
		Eligibility: compile.EligibilityVisibleCommit,
		Authority: compile.AuthorityScope{
			Mount:      "vol",
			MountKeyID: 1,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.BucketForInodeID(fsmeta.RootInode)},
			Parents:    []fsmeta.InodeID{fsmeta.RootInode},
			Inodes:     []fsmeta.InodeID{22},
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if err := executor.admitPerasAuthority(ctx, delta); err != nil {
			b.Fatal(err)
		}
	}
}
