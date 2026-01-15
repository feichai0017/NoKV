package latch_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/percolator/latch"
)

func TestManagerSerializesConflicts(t *testing.T) {
	mgr := latch.NewManager(8)
	keys := [][]byte{[]byte("a"), []byte("b")}
	guard := mgr.Acquire(keys)
	blocked := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mgr.Acquire([][]byte{[]byte("b")}).Release()
		blocked <- struct{}{}
	}()

	select {
	case <-blocked:
		t.Fatalf("latch acquisition should block")
	default:
	}

	guard.Release()
	wg.Wait()
	select {
	case <-blocked:
	default:
		t.Fatalf("latch acquisition did not proceed after release")
	}
}

func TestManagerIgnoresEmptyKeys(t *testing.T) {
	mgr := latch.NewManager(4)
	guard := mgr.Acquire(nil)
	require.NotNil(t, guard)
	guard.Release()
}
