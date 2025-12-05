package utils

import (
	"runtime"
	"testing"
)

func TestRingBasic(t *testing.T) {
	r := NewRing[int](4)
	if !r.Push(1) || !r.Push(2) || !r.Push(3) || !r.Push(4) {
		t.Fatalf("expected pushes to succeed")
	}
	if r.Push(5) { // full
		t.Fatalf("push should fail when full")
	}
	if r.Len() != 4 || r.Cap() < 4 {
		t.Fatalf("unexpected len/cap %d/%d", r.Len(), r.Cap())
	}
	for i := 1; i <= 4; i++ {
		v, ok := r.Pop()
		if !ok || v != i {
			t.Fatalf("pop got %v %v at %d", v, ok, i)
		}
	}
	if _, ok := r.Pop(); ok {
		t.Fatalf("expected empty pop")
	}
}

func TestRingConcurrent(t *testing.T) {
	r := NewRing[int](64)
	done := make(chan struct{})
	// producer
	go func() {
		for i := 0; i < 1000; i++ {
			for !r.Push(i) {
				runtime.Gosched()
			}
		}
		r.Close()
		close(done)
	}()
	count := 0
	for {
		v, ok := r.Pop()
		if !ok {
			if r.Closed() {
				break
			}
			runtime.Gosched()
			continue
		}
		_ = v
		count++
	}
	<-done
	if count != 1000 {
		t.Fatalf("expected 1000, got %d", count)
	}
}
