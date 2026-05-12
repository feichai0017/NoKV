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

func opWithReads(keys ...string) compile.MaterializedOp {
	delta := compile.SemanticDelta{Eligibility: compile.EligibilityVisibleCommit}
	for _, key := range keys {
		delta.ReadPredicates = append(delta.ReadPredicates, compile.Predicate{
			Kind: compile.PredicateObservedValue,
			Key:  []byte(key),
		})
	}
	return compile.MaterializeDelta(delta, nil)
}

func opWithPrefixRead(prefix string) compile.MaterializedOp {
	return compile.MaterializeDelta(compile.SemanticDelta{
		Eligibility: compile.EligibilityVisibleCommit,
		ReadPredicates: []compile.Predicate{{
			Kind: compile.PredicatePrefixScan,
			Key:  []byte(prefix),
		}},
	}, nil)
}

func opWithWrites(keys ...string) compile.MaterializedOp {
	delta := compile.SemanticDelta{Eligibility: compile.EligibilityVisibleCommit}
	for _, key := range keys {
		delta.WriteEffects = append(delta.WriteEffects, compile.WriteEffect{
			Kind: compile.EffectPut,
			Key:  []byte(key),
		})
	}
	return compile.MaterializeDelta(delta, nil)
}

func keyForBench(i int) string {
	return string([]byte{'k', byte(i)})
}
