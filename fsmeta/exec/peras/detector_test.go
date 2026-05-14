// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestConflictDetectorTracksExactPredecessors(t *testing.T) {
	detector := NewConflictDetector()
	first := opID("c1", 1)
	second := opID("c2", 1)
	third := opID("c3", 1)

	predecessors, err := detector.Admit(first, opWithWrites("a"))
	require.NoError(t, err)
	require.Empty(t, predecessors)

	predecessors, err = detector.Admit(second, opWithWrites("b"))
	require.NoError(t, err)
	require.Empty(t, predecessors)

	predecessors, err = detector.Admit(third, opWithWrites("a"))
	require.NoError(t, err)
	require.Equal(t, []OperationID{first}, predecessors)
}

func TestConflictDetectorCoversReadWriteOrientations(t *testing.T) {
	for _, tc := range []struct {
		name string
		left compile.MaterializedOp
		next compile.MaterializedOp
	}{
		{name: "write write", left: opWithWrites("a"), next: opWithWrites("a")},
		{name: "write read", left: opWithWrites("a"), next: opWithReads("a")},
		{name: "read write", left: opWithReads("a"), next: opWithWrites("a")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			detector := NewConflictDetector()
			first := opID("c1", 1)
			second := opID("c2", 1)
			_, err := detector.Admit(first, tc.left)
			require.NoError(t, err)

			predecessors, err := detector.Admit(second, tc.next)
			require.NoError(t, err)
			require.Equal(t, []OperationID{first}, predecessors)
		})
	}
}

func TestConflictDetectorHandlesPrefixPredicates(t *testing.T) {
	detector := NewConflictDetector()
	first := opID("c1", 1)
	second := opID("c2", 1)
	third := opID("c3", 1)

	_, err := detector.Admit(first, opWithPrefixRead("dir/"))
	require.NoError(t, err)

	predecessors, err := detector.Admit(second, opWithWrites("dir/name"))
	require.NoError(t, err)
	require.Equal(t, []OperationID{first}, predecessors)

	predecessors, err = detector.Admit(third, opWithWrites("other/name"))
	require.NoError(t, err)
	require.Empty(t, predecessors)
}

func TestConflictDetectorRemoveRetiresPendingOperation(t *testing.T) {
	detector := NewConflictDetector()
	first := opID("c1", 1)
	second := opID("c2", 1)
	require.NoError(t, mustAdmit(detector, first, opWithWrites("a")))

	detector.Remove(first)

	predecessors, err := detector.Admit(second, opWithWrites("a"))
	require.NoError(t, err)
	require.Empty(t, predecessors)
	require.Equal(t, 1, detector.Len())
}

func TestConflictDetectorRejectsInvalidAndDuplicateIDs(t *testing.T) {
	detector := NewConflictDetector()
	_, err := detector.Admit(OperationID{}, opWithWrites("a"))
	require.ErrorIs(t, err, ErrInvalidOperationID)
	_, err = detector.Admit(OperationID{ClientID: "c1"}, opWithWrites("a"))
	require.ErrorIs(t, err, ErrInvalidOperationID)
	_, err = detector.Admit(OperationID{Seq: 1}, opWithWrites("a"))
	require.ErrorIs(t, err, ErrInvalidOperationID)

	id := opID("c1", 1)
	require.NoError(t, mustAdmit(detector, id, opWithWrites("a")))
	_, err = detector.Admit(id, opWithWrites("b"))
	require.ErrorIs(t, err, ErrDuplicateOperation)
}

func BenchmarkConflictDetectorAdmitDisjoint(b *testing.B) {
	detector := NewConflictDetector()
	for i := range 64 {
		if err := mustAdmit(detector, opID("seed", uint64(i+1)), opWithWrites(keyForBench(i))); err != nil {
			b.Fatal(err)
		}
	}
	id := opID("bench", 1)
	op := opWithWrites("bench-key")

	b.ReportAllocs()
	for b.Loop() {
		predecessors, err := detector.Admit(id, op)
		if err != nil {
			b.Fatal(err)
		}
		if len(predecessors) != 0 {
			b.Fatalf("unexpected predecessors: %v", predecessors)
		}
		detector.Remove(id)
	}
}

func opID(client string, seq uint64) OperationID {
	return OperationID{ClientID: client, Seq: seq}
}

func mustAdmit(detector *ConflictDetector, id OperationID, op compile.MaterializedOp) error {
	_, err := detector.Admit(id, op)
	return err
}

func testFootprintOp(delta compile.SemanticDelta) compile.MaterializedOp {
	if delta.Eligibility == 0 {
		delta.Eligibility = compile.EligibilityVisibleCommit
	}
	reads := make([]compile.KeyRef, 0, len(delta.ReadPredicates))
	writes := make([]compile.KeyRef, 0, len(delta.WriteEffects))
	conflicts := make([]compile.KeyRef, 0, len(delta.ReadPredicates)+len(delta.WriteEffects))
	hasPrefixRead := false
	for _, predicate := range delta.ReadPredicates {
		mode := compile.KeyAccessRead
		if predicate.Kind == compile.PredicatePrefixScan {
			mode = compile.KeyAccessReadPrefix
			hasPrefixRead = true
		}
		ref := compile.KeyRef{Mode: mode, Key: append([]byte(nil), predicate.Key...), Opaque: true}
		reads = append(reads, ref)
		conflicts = append(conflicts, ref)
		if predicate.Kind == compile.PredicatePrefixScan {
			delta.SlowReason = compile.SlowReasonRangeRead
		}
	}
	effects := make([]compile.EffectPlan, 0, len(delta.WriteEffects))
	for i, effect := range delta.WriteEffects {
		ref := compile.KeyRef{Mode: compile.KeyAccessWrite, Key: append([]byte(nil), effect.Key...), Opaque: true}
		writes = append(writes, ref)
		conflicts = append(conflicts, ref)
		effects = append(effects, compile.EffectPlan{
			ID:       compile.MutationID(i),
			Kind:     effect.Kind,
			Key:      append([]byte(nil), effect.Key...),
			Value:    append([]byte(nil), effect.Value...),
			Concrete: effect.Kind == compile.EffectPut || effect.Kind == compile.EffectDelete,
			Opaque:   true,
		})
	}
	return compile.MaterializedOp{CompiledOp: compile.CompiledOp{
		Delta:   delta,
		Effects: effects,
		Footprint: compile.KeyFootprint{
			Reads:         reads,
			Writes:        writes,
			ConflictKeys:  conflicts,
			HasPrefixRead: hasPrefixRead,
			HasOpaqueKeys: true,
		},
	}}
}

func opWithReads(keys ...string) compile.MaterializedOp {
	delta := compile.SemanticDelta{Eligibility: compile.EligibilityVisibleCommit}
	for _, key := range keys {
		delta.ReadPredicates = append(delta.ReadPredicates, compile.Predicate{
			Kind: compile.PredicateObservedValue,
			Key:  []byte(key),
		})
	}
	return testFootprintOp(delta)
}

func opWithPrefixRead(prefix string) compile.MaterializedOp {
	return testFootprintOp(compile.SemanticDelta{
		Eligibility: compile.EligibilityVisibleCommit,
		ReadPredicates: []compile.Predicate{{
			Kind: compile.PredicatePrefixScan,
			Key:  []byte(prefix),
		}},
	})
}

func opWithWrites(keys ...string) compile.MaterializedOp {
	delta := compile.SemanticDelta{Eligibility: compile.EligibilityVisibleCommit}
	for _, key := range keys {
		delta.WriteEffects = append(delta.WriteEffects, compile.WriteEffect{
			Kind: compile.EffectPut,
			Key:  []byte(key),
		})
	}
	return testFootprintOp(delta)
}

func keyForBench(i int) string {
	return string([]byte{'k', byte(i)})
}
