package utils

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMPSCQueueBasic(t *testing.T) {
	q := NewMPSCQueue[int](4)
	if !q.Push(1) || !q.Push(2) || !q.Push(3) || !q.Push(4) {
		t.Fatalf("expected pushes to succeed")
	}
	if q.Len() != 4 || q.Cap() < 4 {
		t.Fatalf("unexpected len/cap %d/%d", q.Len(), q.Cap())
	}
	for i := 1; i <= 4; i++ {
		v, ok := q.TryPop()
		if !ok || v != i {
			t.Fatalf("pop got %v %v at %d", v, ok, i)
		}
	}
	if _, ok := q.TryPop(); ok {
		t.Fatalf("expected empty try-pop")
	}
	if !q.Close() {
		t.Fatalf("close failed")
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected closed drained queue to return false")
	}
	if q.Push(5) {
		t.Fatalf("push should fail after close")
	}
}

func TestMPSCQueueConcurrent(t *testing.T) {
	q := NewMPSCQueue[int](128)
	const (
		producers = 8
		perProd   = 2000
		total     = producers * perProd
	)
	var produced atomic.Int64
	var consumed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := range producers {
		go func(id int) {
			defer wg.Done()
			base := id * perProd
			for i := range perProd {
				if !q.Push(base + i) {
					t.Errorf("push failed before close")
					return
				}
				produced.Add(1)
			}
		}(p)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, ok := q.Pop()
			if !ok {
				return
			}
			consumed.Add(1)
		}
	}()
	wg.Wait()
	q.Close()
	<-done
	if got := produced.Load(); got != total {
		t.Fatalf("produced %d, want %d", got, total)
	}
	if got := consumed.Load(); got != total {
		t.Fatalf("consumed %d, want %d", got, total)
	}
}

func TestMPSCQueueCloseUnblocksFullProducer(t *testing.T) {
	q := NewMPSCQueue[int](2)
	if !q.Push(1) || !q.Push(2) {
		t.Fatalf("initial fill failed")
	}
	done := make(chan bool, 1)
	go func() {
		done <- q.Push(3)
	}()
	for range 100 {
		runtime.Gosched()
	}
	if !q.Close() {
		t.Fatalf("close failed")
	}
	if ok := <-done; ok {
		t.Fatalf("expected blocked producer to fail after close")
	}
}
