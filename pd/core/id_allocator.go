package core

import (
	"fmt"
	"sync/atomic"
)

// IDAllocator allocates globally unique increasing IDs.
type IDAllocator struct {
	next atomic.Uint64
}

// NewIDAllocator creates an allocator whose first allocation is start.
// When start is zero, allocation starts from 1.
func NewIDAllocator(start uint64) *IDAllocator {
	if start == 0 {
		start = 1
	}
	a := &IDAllocator{}
	a.next.Store(start - 1)
	return a
}

// Alloc allocates and returns one ID.
func (a *IDAllocator) Alloc() uint64 {
	if a == nil {
		return 0
	}
	return a.next.Add(1)
}

// Reserve allocates n consecutive IDs and returns [first, last].
func (a *IDAllocator) Reserve(n uint64) (first, last uint64, err error) {
	if a == nil {
		return 0, 0, nil
	}
	if n == 0 {
		return 0, 0, fmt.Errorf("%w: reserve n must be >= 1", ErrInvalidBatch)
	}
	last = a.next.Add(n)
	first = last - n + 1
	return first, last, nil
}

// Current returns the last allocated ID.
func (a *IDAllocator) Current() uint64 {
	if a == nil {
		return 0
	}
	return a.next.Load()
}
