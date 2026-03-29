package utils

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// MPSCQueue is a bounded multi-producer, single-consumer queue.
//
// It uses a per-slot sequence protocol so producers can reserve and publish
// independently without serializing on a global "published" cursor. Blocking is
// only used for full/empty waiting and shutdown signaling.
type MPSCQueue[T any] struct {
	buf       []mpscSlot[T]
	mask      uint64
	capacity  uint64
	head      atomic.Uint64
	tail      atomic.Uint64
	closed    atomic.Bool
	closeOnce sync.Once
	closeCh   chan struct{}
	mu        sync.Mutex
	notEmpty  *sync.Cond
	notFull   *sync.Cond
}

type mpscSlot[T any] struct {
	seq atomic.Uint64
	val T
}

// NewMPSCQueue creates a bounded MPSC queue with at least the given capacity.
// Capacity is rounded up to the next power of two.
func NewMPSCQueue[T any](capacity int) *MPSCQueue[T] {
	if capacity < 2 {
		capacity = 2
	}
	size := nextPow2(uint64(capacity))
	q := &MPSCQueue[T]{
		buf:      make([]mpscSlot[T], size),
		mask:     size - 1,
		capacity: size,
		closeCh:  make(chan struct{}),
	}
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

// Push waits for space and enqueues v. It returns false if the queue has been
// closed before the caller could reserve a slot.
func (q *MPSCQueue[T]) Push(v T) bool {
	if q == nil {
		return false
	}
	for {
		if q.closed.Load() {
			return false
		}
		pos := q.tail.Load()
		slot := &q.buf[pos&q.mask]
		seq := slot.seq.Load()
		diff := int64(seq) - int64(pos)
		if diff == 0 {
			if q.tail.CompareAndSwap(pos, pos+1) {
				slot.val = v
				slot.seq.Store(pos + 1)
				q.notEmpty.Signal()
				return true
			}
			runtime.Gosched()
			continue
		}
		if diff < 0 {
			q.mu.Lock()
			for !q.closed.Load() {
				pos = q.tail.Load()
				slot = &q.buf[pos&q.mask]
				seq = slot.seq.Load()
				if int64(seq)-int64(pos) == 0 {
					break
				}
				q.notFull.Wait()
			}
			q.mu.Unlock()
			continue
		}
		runtime.Gosched()
	}
}

// TryPop returns the next published item without blocking.
func (q *MPSCQueue[T]) TryPop() (T, bool) {
	var zero T
	if q == nil {
		return zero, false
	}
	pos := q.head.Load()
	slot := &q.buf[pos&q.mask]
	seq := slot.seq.Load()
	diff := int64(seq) - int64(pos+1)
	if diff < 0 {
		return zero, false
	}
	val := slot.val
	slot.val = zero
	q.head.Store(pos + 1)
	slot.seq.Store(pos + q.capacity)
	q.notFull.Signal()
	return val, true
}

// Pop waits for the next published item. It returns ok=false only after Close
// has been called and the queue has been fully drained. Callers must ensure a
// single consumer.
func (q *MPSCQueue[T]) Pop() (T, bool) {
	var zero T
	if q == nil {
		return zero, false
	}
	for {
		if val, ok := q.TryPop(); ok {
			return val, true
		}
		if q.drained() {
			return zero, false
		}
		q.mu.Lock()
		for {
			pos := q.head.Load()
			slot := &q.buf[pos&q.mask]
			if int64(slot.seq.Load())-int64(pos+1) >= 0 {
				q.mu.Unlock()
				break
			}
			if q.closed.Load() && q.tail.Load() == pos {
				q.mu.Unlock()
				return zero, false
			}
			q.notEmpty.Wait()
		}
	}
}

// Close stops accepting new producers. Published items can still be drained.
func (q *MPSCQueue[T]) Close() bool {
	if q == nil {
		return false
	}
	swapped := false
	q.closeOnce.Do(func() {
		q.closed.Store(true)
		close(q.closeCh)
		q.notEmpty.Broadcast()
		q.notFull.Broadcast()
		swapped = true
	})
	return swapped
}

// Closed reports whether the queue has been closed.
func (q *MPSCQueue[T]) Closed() bool {
	if q == nil {
		return true
	}
	return q.closed.Load()
}

// CloseCh is closed when the queue stops accepting new producers.
func (q *MPSCQueue[T]) CloseCh() <-chan struct{} {
	if q == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return q.closeCh
}

// Len returns the number of currently reserved-but-not-consumed items.
func (q *MPSCQueue[T]) Len() int {
	if q == nil {
		return 0
	}
	head := q.head.Load()
	tail := q.tail.Load()
	if tail < head {
		return 0
	}
	return int(tail - head)
}

// Cap returns the bounded queue capacity.
func (q *MPSCQueue[T]) Cap() int {
	if q == nil {
		return 0
	}
	return int(q.capacity)
}

func (q *MPSCQueue[T]) drained() bool {
	if q == nil || !q.closed.Load() {
		return false
	}
	head := q.head.Load()
	return q.tail.Load() == head
}
