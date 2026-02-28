package tso

import (
	"fmt"
	"sync/atomic"

	"github.com/feichai0017/NoKV/pd/core"
)

// Allocator provides monotonic timestamp allocation for PD-lite.
type Allocator struct {
	counter atomic.Uint64
}

// NewAllocator creates a timestamp allocator whose first allocation is start.
// When start is zero, allocation starts from 1.
func NewAllocator(start uint64) *Allocator {
	if start == 0 {
		start = 1
	}
	a := &Allocator{}
	a.counter.Store(start - 1)
	return a
}

// Next allocates one timestamp.
func (a *Allocator) Next() uint64 {
	if a == nil {
		return 0
	}
	return a.counter.Add(1)
}

// Reserve allocates n consecutive timestamps and returns (first, count).
func (a *Allocator) Reserve(n uint64) (first, count uint64, err error) {
	if a == nil {
		return 0, 0, nil
	}
	if n == 0 {
		return 0, 0, fmt.Errorf("%w: tso reserve n must be >= 1", core.ErrInvalidBatch)
	}
	last := a.counter.Add(n)
	first = last - n + 1
	return first, n, nil
}

// Current returns the latest allocated timestamp.
func (a *Allocator) Current() uint64 {
	if a == nil {
		return 0
	}
	return a.counter.Load()
}
