package peras

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestAdmissionLatchesSerializeOverlappingKeys(t *testing.T) {
	latches := NewAdmissionLatches()
	op := opWithValueWrites("dentry/a", "inode=7")

	release := latches.Lock(op)
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		unlock := latches.Lock(op)
		close(entered)
		unlock()
		close(done)
	}()

	select {
	case <-entered:
		t.Fatal("overlapping admission entered while key was held")
	case <-time.After(20 * time.Millisecond):
	}
	release()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("overlapping admission did not resume")
	}
}

func TestAdmissionLatchesAllowDisjointKeys(t *testing.T) {
	latches := NewAdmissionLatches()
	release := latches.Lock(opWithValueWrites("dentry/a", "inode=7"))
	defer release()

	done := make(chan struct{})
	go func() {
		unlock := latches.Lock(opWithValueWrites("dentry/b", "inode=8"))
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disjoint admission was blocked")
	}
}

func TestAdmissionLatchesUseGlobalKeyForPrefixPredicates(t *testing.T) {
	latches := NewAdmissionLatches()
	release := latches.Lock(compile.MaterializeDelta(compile.SemanticDelta{
		ReadPredicates: []compile.Predicate{{Kind: compile.PredicatePrefixScan, Key: []byte("dentry/")}},
	}, nil))

	var entered atomic.Bool
	done := make(chan struct{})
	go func() {
		unlock := latches.Lock(opWithValueWrites("dentry/b", "inode=8"))
		entered.Store(true)
		unlock()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("global admission latch did not block ordinary key")
	case <-time.After(20 * time.Millisecond):
		require.False(t, entered.Load())
	}
	release()
	<-done
}

func TestAdmitRejectsFalseAdmission(t *testing.T) {
	err := Admit(context.Background(), compile.MaterializeDelta(compile.SemanticDelta{}, nil), func(context.Context, compile.MaterializedOp) (bool, error) {
		return false, nil
	})
	require.ErrorIs(t, err, ErrAdmissionRejected)
}
