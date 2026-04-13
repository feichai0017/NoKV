package utils

import (
	"fmt"
	"sync/atomic"
)

// RefCount provides atomic reference counting with fail-fast underflow
// detection. It uses a CAS loop that guarantees the counter never goes
// below zero, even if a panic is recovered by the caller.
//
// Types that manage lifecycle via reference counting should embed this
// struct and provide their own DecrRef that calls Decr and performs
// type-specific cleanup when the count reaches zero:
//
//	func (x *MyType) DecrRef() {
//	    if x.Decr() == 0 {
//	        x.cleanup()
//	    }
//	}
type RefCount struct {
	ref atomic.Int32
}

// Incr atomically increments the reference count by one.
func (r *RefCount) Incr() {
	r.ref.Add(1)
}

// Decr atomically decrements the reference count by one and returns
// the new value. It panics if the count would go below zero, which
// indicates a lifecycle bug in the caller.
func (r *RefCount) Decr() int32 {
	for {
		cur := r.ref.Load()
		if cur <= 0 {
			panic(fmt.Sprintf("RefCount.Decr: refcount underflow (current=%d)", cur))
		}
		if r.ref.CompareAndSwap(cur, cur-1) {
			return cur - 1
		}
	}
}

// Reset sets the reference count to zero.
// Intended for object-pool re-acquisition paths.
func (r *RefCount) Reset() {
	r.ref.Store(0)
}

// Init sets the reference count to the given value.
// Intended for constructor or factory paths that need a non-zero initial count.
func (r *RefCount) Init(n int32) {
	r.ref.Store(n)
}

// Load returns the current reference count. Intended for tests and
// diagnostics only; callers must not make lifecycle decisions based on
// the returned value because it may be stale.
func (r *RefCount) Load() int32 {
	return r.ref.Load()
}
