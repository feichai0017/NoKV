package utils

import (
	"runtime"
	"sync/atomic"
)

// Ring is a fixed-size MPSC ring buffer with lock-free push/pop.
// Capacity must be a power of two; constructor will round up.
type Ring[T any] struct {
	buf    []T
	mask   uint64
	head   uint64 // next slot to pop
	tail   uint64 // next slot to push
	closed atomic.Bool
}

// NewRing creates a ring buffer with at least the given capacity.
// Capacity is rounded up to the next power of two.
func NewRing[T any](capacity int) *Ring[T] {
	if capacity < 2 {
		capacity = 2
	}
	size := nextPow2(uint64(capacity))
	return &Ring[T]{
		buf:  make([]T, size),
		mask: size - 1,
	}
}

// Push inserts v; returns false if the ring is full or closed.
func (r *Ring[T]) Push(v T) bool {
	if r == nil || r.closed.Load() {
		return false
	}
	for {
		tail := atomic.LoadUint64(&r.tail)
		head := atomic.LoadUint64(&r.head)
		if tail-head >= r.mask+1 {
			return false
		}
		if atomic.CompareAndSwapUint64(&r.tail, tail, tail+1) {
			r.buf[tail&r.mask] = v
			return true
		}
		runtime.Gosched()
	}
}

// Pop removes and returns an element. ok is false if empty or closed.
func (r *Ring[T]) Pop() (val T, ok bool) {
	if r == nil {
		return val, false
	}
	for {
		head := atomic.LoadUint64(&r.head)
		tail := atomic.LoadUint64(&r.tail)
		if head == tail {
			return val, false
		}
		if atomic.CompareAndSwapUint64(&r.head, head, head+1) {
			val = r.buf[head&r.mask]
			return val, true
		}
		runtime.Gosched()
	}
}

// Close marks the ring as closed; Push will fail and Pop returns empty when drained.
func (r *Ring[T]) Close() {
	if r == nil {
		return
	}
	r.closed.Store(true)
}

// Closed reports whether the ring has been closed.
func (r *Ring[T]) Closed() bool {
	if r == nil {
		return true
	}
	return r.closed.Load()
}

// Len returns the current number of elements.
func (r *Ring[T]) Len() int {
	if r == nil {
		return 0
	}
	head := atomic.LoadUint64(&r.head)
	tail := atomic.LoadUint64(&r.tail)
	if tail < head {
		return 0
	}
	return int(tail - head)
}

// Cap returns buffer capacity.
func (r *Ring[T]) Cap() int {
	if r == nil {
		return 0
	}
	return len(r.buf)
}

func nextPow2(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}
