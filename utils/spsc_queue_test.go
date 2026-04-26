package utils

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSPSCQueueCapacityRoundsUp(t *testing.T) {
	cases := map[int]int{
		0:  2,
		1:  2,
		2:  2,
		3:  4,
		5:  8,
		8:  8,
		15: 16,
	}
	for in, want := range cases {
		q := NewSPSCQueue[int](in)
		if q.Cap() != want {
			t.Fatalf("cap(%d): got %d want %d", in, q.Cap(), want)
		}
	}
}

func TestSPSCQueuePushPop(t *testing.T) {
	q := NewSPSCQueue[int](4)
	for i := 1; i <= 4; i++ {
		if !q.Push(i) {
			t.Fatalf("push %d returned false unexpectedly", i)
		}
	}
	if q.Push(5) {
		t.Fatalf("push to full queue should return false")
	}
	if got := q.Len(); got != 4 {
		t.Fatalf("Len: got %d want 4", got)
	}
	for i := 1; i <= 4; i++ {
		v, ok := q.TryPop()
		if !ok || v != i {
			t.Fatalf("pop %d: got %v ok=%v", i, v, ok)
		}
	}
	if _, ok := q.TryPop(); ok {
		t.Fatalf("expected empty TryPop")
	}
}

func TestSPSCQueueBlockingPopWakesOnPush(t *testing.T) {
	q := NewSPSCQueue[int](2)
	got := make(chan int, 1)
	go func() {
		v, ok := q.BlockingPop()
		if !ok {
			t.Errorf("blocking pop returned closed unexpectedly")
		}
		got <- v
	}()
	// Give the consumer time to park on q.notify.
	runtime.Gosched()
	time.Sleep(5 * time.Millisecond)
	if !q.Push(42) {
		t.Fatalf("push failed")
	}
	select {
	case v := <-got:
		if v != 42 {
			t.Fatalf("got %d want 42", v)
		}
	case <-time.After(time.Second):
		t.Fatalf("blocking pop did not wake")
	}
}

func TestSPSCQueueCloseDrainsThenSignals(t *testing.T) {
	q := NewSPSCQueue[int](4)
	if !q.Push(1) || !q.Push(2) {
		t.Fatalf("push failed")
	}
	q.Close()
	if !q.Closed() {
		t.Fatalf("closed flag should be true")
	}
	// Closed queue still drains buffered items.
	v, ok := q.BlockingPop()
	if !ok || v != 1 {
		t.Fatalf("first drain after close: got %d ok=%v", v, ok)
	}
	v, ok = q.BlockingPop()
	if !ok || v != 2 {
		t.Fatalf("second drain after close: got %d ok=%v", v, ok)
	}
	// Drained: BlockingPop must return ok=false.
	if _, ok := q.BlockingPop(); ok {
		t.Fatalf("expected closed+empty BlockingPop to return false")
	}
}

func TestSPSCQueueCloseUnblocksPendingPop(t *testing.T) {
	q := NewSPSCQueue[int](2)
	done := make(chan bool, 1)
	go func() {
		_, ok := q.BlockingPop()
		done <- ok
	}()
	runtime.Gosched()
	time.Sleep(5 * time.Millisecond)
	q.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatalf("expected ok=false after close")
		}
	case <-time.After(time.Second):
		t.Fatalf("Close did not unblock pending BlockingPop")
	}
}

func TestSPSCQueuePushAfterCloseFails(t *testing.T) {
	q := NewSPSCQueue[int](2)
	q.Close()
	if q.Push(1) {
		t.Fatalf("push to closed queue should fail")
	}
}

func TestSPSCQueueWrapsCorrectly(t *testing.T) {
	q := NewSPSCQueue[int](4)
	// Cycle through the ring multiple times to exercise the wrap mask.
	for round := 0; round < 100; round++ {
		for i := 0; i < 3; i++ {
			if !q.Push(round*10 + i) {
				t.Fatalf("push round=%d i=%d failed", round, i)
			}
		}
		for i := 0; i < 3; i++ {
			v, ok := q.TryPop()
			if !ok || v != round*10+i {
				t.Fatalf("pop round=%d i=%d: got %d ok=%v", round, i, v, ok)
			}
		}
	}
}

func TestSPSCQueueProducerConsumer(t *testing.T) {
	const N = 50_000
	q := NewSPSCQueue[int](64)
	var sum atomic.Int64

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			v, ok := q.BlockingPop()
			if !ok {
				return
			}
			sum.Add(int64(v))
		}
	}()

	for i := 1; i <= N; i++ {
		for !q.Push(i) {
			runtime.Gosched()
		}
	}
	q.Close()
	wg.Wait()

	want := int64(N) * int64(N+1) / 2
	if got := sum.Load(); got != want {
		t.Fatalf("sum mismatch: got %d want %d", got, want)
	}
}
