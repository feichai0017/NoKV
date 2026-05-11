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

	predecessors, err := detector.Admit(first, deltaWithWrites("a"))
	require.NoError(t, err)
	require.Empty(t, predecessors)

	predecessors, err = detector.Admit(second, deltaWithWrites("b"))
	require.NoError(t, err)
	require.Empty(t, predecessors)

	predecessors, err = detector.Admit(third, deltaWithWrites("a"))
	require.NoError(t, err)
	require.Equal(t, []OperationID{first}, predecessors)
}

func TestConflictDetectorCoversReadWriteOrientations(t *testing.T) {
	for _, tc := range []struct {
		name string
		left compile.SemanticDelta
		next compile.SemanticDelta
	}{
		{name: "write write", left: deltaWithWrites("a"), next: deltaWithWrites("a")},
		{name: "write read", left: deltaWithWrites("a"), next: deltaWithReads("a")},
		{name: "read write", left: deltaWithReads("a"), next: deltaWithWrites("a")},
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

	_, err := detector.Admit(first, deltaWithPrefixRead("dir/"))
	require.NoError(t, err)

	predecessors, err := detector.Admit(second, deltaWithWrites("dir/name"))
	require.NoError(t, err)
	require.Equal(t, []OperationID{first}, predecessors)

	predecessors, err = detector.Admit(third, deltaWithWrites("other/name"))
	require.NoError(t, err)
	require.Empty(t, predecessors)
}

func TestConflictDetectorRemoveRetiresPendingOperation(t *testing.T) {
	detector := NewConflictDetector()
	first := opID("c1", 1)
	second := opID("c2", 1)
	require.NoError(t, mustAdmit(detector, first, deltaWithWrites("a")))

	detector.Remove(first)

	predecessors, err := detector.Admit(second, deltaWithWrites("a"))
	require.NoError(t, err)
	require.Empty(t, predecessors)
	require.Equal(t, 1, detector.Len())
}

func TestConflictDetectorRejectsInvalidAndDuplicateIDs(t *testing.T) {
	detector := NewConflictDetector()
	_, err := detector.Admit(OperationID{}, deltaWithWrites("a"))
	require.ErrorIs(t, err, ErrInvalidOperationID)

	id := opID("c1", 1)
	require.NoError(t, mustAdmit(detector, id, deltaWithWrites("a")))
	_, err = detector.Admit(id, deltaWithWrites("b"))
	require.ErrorIs(t, err, ErrDuplicateOperation)
}

func BenchmarkConflictDetectorAdmitDisjoint(b *testing.B) {
	detector := NewConflictDetector()
	for i := range 64 {
		if err := mustAdmit(detector, opID("seed", uint64(i+1)), deltaWithWrites(keyForBench(i))); err != nil {
			b.Fatal(err)
		}
	}
	id := opID("bench", 1)
	delta := deltaWithWrites("bench-key")

	b.ReportAllocs()
	for b.Loop() {
		predecessors, err := detector.Admit(id, delta)
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

func mustAdmit(detector *ConflictDetector, id OperationID, delta compile.SemanticDelta) error {
	_, err := detector.Admit(id, delta)
	return err
}

func deltaWithReads(keys ...string) compile.SemanticDelta {
	delta := compile.SemanticDelta{Eligibility: compile.EligibilityFastPath}
	for _, key := range keys {
		delta.ReadPredicates = append(delta.ReadPredicates, compile.Predicate{
			Kind: compile.PredicateObservedValue,
			Key:  []byte(key),
		})
	}
	return delta
}

func deltaWithPrefixRead(prefix string) compile.SemanticDelta {
	return compile.SemanticDelta{
		Eligibility: compile.EligibilityFastPath,
		ReadPredicates: []compile.Predicate{{
			Kind: compile.PredicatePrefixScan,
			Key:  []byte(prefix),
		}},
	}
}

func deltaWithWrites(keys ...string) compile.SemanticDelta {
	delta := compile.SemanticDelta{Eligibility: compile.EligibilityFastPath}
	for _, key := range keys {
		delta.WriteEffects = append(delta.WriteEffects, compile.WriteEffect{
			Kind: compile.EffectPut,
			Key:  []byte(key),
		})
	}
	return delta
}

func keyForBench(i int) string {
	return string([]byte{'k', byte(i)})
}
