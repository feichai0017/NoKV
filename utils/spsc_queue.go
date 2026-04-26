package utils

import (
	"sync/atomic"
)

// SPSCQueue is a fixed-capacity ring backed by atomic head/tail indices.
// Exactly one producer goroutine may call Push, and exactly one consumer
// goroutine may call TryPop / BlockingPop. Capacity is rounded up to the
// next power of two so head/tail wrap with a single mask.
//
// It exists to replace `chan T` on hot single-producer/single-consumer
// hand-offs (e.g. NoKV's per-shard commit dispatch path) where Go
// channels show up as scheduler overhead under high concurrency: each
// send/receive pair touches the runtime even when neither side parks.
// Push and TryPop are wait-free; the only scheduler interaction is a
// 1-cap signal channel that producers fire only on the empty→non-empty
// transition, so a long burst of N pushes produces at most one wake-up.
type SPSCQueue[T any] struct {
	buf    []T
	mask   uint64
	head   atomic.Uint64 // consumer-owned write side
	tail   atomic.Uint64 // producer-owned write side
	// parked indicates the consumer has set up to sleep on notify and
	// has already verified the ring was empty after that setup. The
	// producer reads it to decide whether a wake-up is needed; signaling
	// a non-parked consumer is a wasted system call. Consumer clears it
	// before any work that may not park (TryPop returning successfully).
	parked atomic.Bool
	closed atomic.Bool
	// notify is fired by the producer when parked is observed true, and
	// by Close. Cap=1 buffer absorbs spurious sends (the consumer drains
	// it via <-notify the next time it parks).
	notify chan struct{}
}

// NewSPSCQueue returns a queue with capacity rounded up to a power of two
// (minimum 2).
func NewSPSCQueue[T any](capacity int) *SPSCQueue[T] {
	if capacity < 2 {
		capacity = 2
	}
	if capacity&(capacity-1) != 0 {
		c := 1
		for c < capacity {
			c <<= 1
		}
		capacity = c
	}
	return &SPSCQueue[T]{
		buf:    make([]T, capacity),
		mask:   uint64(capacity - 1),
		notify: make(chan struct{}, 1),
	}
}

// Cap returns the queue capacity (always a power of two).
func (q *SPSCQueue[T]) Cap() int { return len(q.buf) }

// Len returns the current number of items waiting to be popped. The
// consumer can rely on Len for sizing decisions; the producer sees a
// snapshot that may shrink concurrently as the consumer drains.
func (q *SPSCQueue[T]) Len() int {
	return int(q.tail.Load() - q.head.Load())
}

// Push enqueues v from the single producer. Returns false if the queue
// is full or closed. Push itself does not block — the caller chooses
// whether to retry (with a yield) or drop on a false return.
func (q *SPSCQueue[T]) Push(v T) bool {
	if q.closed.Load() {
		return false
	}
	head := q.head.Load()
	tail := q.tail.Load()
	if tail-head > q.mask {
		return false
	}
	q.buf[tail&q.mask] = v
	q.tail.Store(tail + 1)
	// Wake the consumer only if it has parked. The double-check pattern
	// in BlockingPop closes the race where the consumer set parked=true
	// but had not yet observed our tail.Store: that consumer will
	// retry TryPop after this wake, see the new item, and return.
	if q.parked.Load() {
		q.parked.Store(false)
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
	return true
}

// TryPop dequeues from the single consumer without blocking. ok=false
// means the queue is empty.
func (q *SPSCQueue[T]) TryPop() (T, bool) {
	var zero T
	head := q.head.Load()
	tail := q.tail.Load()
	if head == tail {
		return zero, false
	}
	v := q.buf[head&q.mask]
	q.buf[head&q.mask] = zero // release reference for GC
	q.head.Store(head + 1)
	return v, true
}

// BlockingPop dequeues, waiting on the notify channel when the queue is
// empty. Returns ok=false once the queue has been closed AND drained.
func (q *SPSCQueue[T]) BlockingPop() (T, bool) {
	var zero T
	for {
		if v, ok := q.TryPop(); ok {
			return v, true
		}
		if q.closed.Load() {
			if v, ok := q.TryPop(); ok {
				return v, true
			}
			return zero, false
		}
		// Mark parked, then re-check the ring before sleeping. This
		// closes the race where a producer pushed (and skipped wake-up
		// because parked was still false) right before we set it.
		q.parked.Store(true)
		if v, ok := q.TryPop(); ok {
			q.parked.Store(false)
			return v, true
		}
		if q.closed.Load() {
			q.parked.Store(false)
			if v, ok := q.TryPop(); ok {
				return v, true
			}
			return zero, false
		}
		<-q.notify
		// Producer/Close may already have cleared parked; ensure it.
		q.parked.Store(false)
	}
}

// Close signals consumers to stop after the queue drains. Producers
// MUST stop pushing before calling Close; calling Close concurrently
// with Push is undefined.
func (q *SPSCQueue[T]) Close() {
	if q.closed.Swap(true) {
		return
	}
	q.parked.Store(false)
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Closed reports whether Close has been called.
func (q *SPSCQueue[T]) Closed() bool { return q.closed.Load() }
