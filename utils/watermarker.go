package utils

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
)

type uint64Heap []uint64

func (u uint64Heap) Len() int           { return len(u) }
func (u uint64Heap) Less(i, j int) bool { return u[i] < u[j] }
func (u uint64Heap) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x any)        { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() any {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

// WaterMark is used to keep track of the minimum un-finished index.  Typically, an index k becomes
// finished or "done" according to a WaterMark once Done(k) has been called
//  1. as many times as Begin(k) has, AND
//  2. a positive number of times.
//
// An index may also become "done" by calling SetDoneUntil at a time such that it is not
// inter-mingled with Begin/Done calls.
//
// Since doneUntil and lastIndex addresses are passed to sync/atomic packages, we ensure that they
// are 64-bit aligned by putting them at the beginning of the structure.
type WaterMark struct {
	doneUntil uint64
	lastIndex uint64
	Name      string

	mu      sync.Mutex
	pending map[uint64]int
	waiters map[uint64][]chan struct{}
	indices uint64Heap
}

// Init initializes a WaterMark struct. MUST be called before using it.
func (w *WaterMark) Init(closer *Closer) {
	const defaultCap = 128
	w.pending = make(map[uint64]int, defaultCap)
	w.waiters = make(map[uint64][]chan struct{}, defaultCap)
	w.indices = make(uint64Heap, 0, defaultCap)
	heap.Init(&w.indices)
	// Legacy closers expected each watermark processor to call Done once.
	// We no longer run a background goroutine, so mark it done immediately
	// to avoid leaking waiters on shutdown.
	if closer != nil {
		closer.Done()
	}
}

// Begin sets the last index to the given value.
func (w *WaterMark) Begin(index uint64) {
	atomic.StoreUint64(&w.lastIndex, index)
	w.addIndex(index, 1)
}

// BeginMany works like Begin but accepts multiple indices.
func (w *WaterMark) BeginMany(indices []uint64) {
	atomic.StoreUint64(&w.lastIndex, indices[len(indices)-1])
	for _, idx := range indices {
		w.addIndex(idx, 1)
	}
}

// Done sets a single index as done.
func (w *WaterMark) Done(index uint64) {
	w.addIndex(index, -1)
}

// DoneMany works like Done but accepts multiple indices.
func (w *WaterMark) DoneMany(indices []uint64) {
	for _, idx := range indices {
		w.addIndex(idx, -1)
	}
}

// DoneUntil returns the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) DoneUntil() uint64 {
	return atomic.LoadUint64(&w.doneUntil)
}

// SetDoneUntil sets the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) SetDoneUntil(val uint64) {
	prev := atomic.SwapUint64(&w.doneUntil, val)
	w.mu.Lock()
	w.notifyWaitersLocked(prev, val)
	w.mu.Unlock()
}

// LastIndex returns the last index for which Begin has been called.
func (w *WaterMark) LastIndex() uint64 {
	return atomic.LoadUint64(&w.lastIndex)
}

// WaitForMark waits until the given index is marked as done.
func (w *WaterMark) WaitForMark(ctx context.Context, index uint64) error {
	if w.DoneUntil() >= index {
		return nil
	}
	waitCh := make(chan struct{})

	w.mu.Lock()
	if w.DoneUntil() >= index {
		w.mu.Unlock()
		return nil
	}
	w.waiters[index] = append(w.waiters[index], waitCh)
	w.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (w *WaterMark) addIndex(index uint64, delta int) {
	if index == 0 {
		return
	}
	w.mu.Lock()
	if _, present := w.pending[index]; !present {
		heap.Push(&w.indices, index)
	}
	w.pending[index] += delta
	w.advanceLocked()
	w.mu.Unlock()
}

func (w *WaterMark) advanceLocked() {
	doneUntil := w.DoneUntil()
	until := doneUntil

	for len(w.indices) > 0 {
		min := w.indices[0]
		if pending := w.pending[min]; pending > 0 {
			break
		}
		heap.Pop(&w.indices)
		delete(w.pending, min)
		until = min
	}

	if until != doneUntil {
		atomic.StoreUint64(&w.doneUntil, until)
		w.notifyWaitersLocked(doneUntil, until)
	}
}

func (w *WaterMark) notifyWaitersLocked(prev, until uint64) {
	notify := func(idx uint64, chans []chan struct{}) {
		for _, ch := range chans {
			close(ch)
		}
		delete(w.waiters, idx)
	}

	// When waiters are sparse, iterate by index distance; otherwise, range over map.
	if until-prev <= uint64(len(w.waiters)) {
		for idx := prev + 1; idx <= until; idx++ {
			if chans, ok := w.waiters[idx]; ok {
				notify(idx, chans)
			}
		}
		return
	}

	for idx, chans := range w.waiters {
		if idx <= until {
			notify(idx, chans)
		}
	}
}
