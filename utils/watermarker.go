package utils

import (
	"context"
	"sync"
	"sync/atomic"
)

const defaultWatermarkWindow = 1 << 16

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
	waiters map[uint64]chan struct{}
	window  atomic.Value // *watermarkWindow
}

type watermarkWindow struct {
	base  uint64
	slots []atomic.Int32
}

// Init initializes a WaterMark struct. MUST be called before using it.
func (w *WaterMark) Init(closer *Closer) {
	const defaultCap = 128
	w.waiters = make(map[uint64]chan struct{}, defaultCap)
	w.window.Store(&watermarkWindow{
		base:  1,
		slots: make([]atomic.Int32, defaultWatermarkWindow),
	})
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
	w.mu.Lock()
	if w.DoneUntil() >= index {
		w.mu.Unlock()
		return nil
	}
	waitCh, ok := w.waiters[index]
	if !ok {
		waitCh = make(chan struct{})
		w.waiters[index] = waitCh
	}
	w.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (w *WaterMark) addIndex(index uint64, delta int32) {
	if index == 0 {
		return
	}
	win := w.ensureWindow(index)
	offset := index - win.base
	if offset < uint64(len(win.slots)) {
		win.slots[offset].Add(delta)
	}
	w.tryAdvance()
}

func (w *WaterMark) tryAdvance() {
	for {
		doneUntil := w.DoneUntil()
		lastIndex := w.LastIndex()
		if doneUntil >= lastIndex {
			return
		}
		next := doneUntil + 1
		win := w.loadWindow()
		if next < win.base || next >= win.base+uint64(len(win.slots)) {
			w.ensureWindow(next)
			continue
		}
		offset := next - win.base
		if win.slots[offset].Load() > 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&w.doneUntil, doneUntil, next) {
			w.notifyWaiters(doneUntil, next)
			continue
		}
	}
}

func (w *WaterMark) notifyWaitersLocked(_ uint64, until uint64) {
	for idx, ch := range w.waiters {
		if idx <= until {
			close(ch)
			delete(w.waiters, idx)
		}
	}
}

func (w *WaterMark) notifyWaiters(prev, until uint64) {
	w.mu.Lock()
	w.notifyWaitersLocked(prev, until)
	w.mu.Unlock()
}

func (w *WaterMark) ensureWindow(index uint64) *watermarkWindow {
	win := w.loadWindow()
	if index >= win.base && index < win.base+uint64(len(win.slots)) {
		return win
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	win = w.loadWindow()
	if index >= win.base && index < win.base+uint64(len(win.slots)) {
		return win
	}
	w.rebuildWindowLocked(index, win)
	return w.loadWindow()
}

func (w *WaterMark) rebuildWindowLocked(index uint64, win *watermarkWindow) {
	done := w.DoneUntil()
	newBase := done + 1
	if index < newBase {
		index = newBase
	}
	size := len(win.slots)
	if size == 0 {
		size = defaultWatermarkWindow
	}
	needed := index - newBase + 1
	for uint64(size) < needed {
		size <<= 1
	}
	newSlots := make([]atomic.Int32, size)
	for i := range win.slots {
		count := win.slots[i].Load()
		if count == 0 {
			continue
		}
		idx := win.base + uint64(i)
		if idx < newBase {
			continue
		}
		offset := idx - newBase
		if offset >= uint64(size) {
			continue
		}
		newSlots[offset].Store(count)
	}
	w.window.Store(&watermarkWindow{
		base:  newBase,
		slots: newSlots,
	})
}

func (w *WaterMark) loadWindow() *watermarkWindow {
	if w.window.Load() == nil {
		win := &watermarkWindow{
			base:  1,
			slots: make([]atomic.Int32, defaultWatermarkWindow),
		}
		w.window.Store(win)
		return win
	}
	return w.window.Load().(*watermarkWindow)
}
