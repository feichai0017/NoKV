package NoKV

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/kv"
)

type request struct {
	Entries    []*kv.Entry
	Ptrs       []kv.ValuePtr
	ptrIdxs    []int
	ptrBuckets []uint32
	Err        error
	kv.RefCount
	enqueueAt time.Time
	wg        sync.WaitGroup
}

var requestPool = sync.Pool{
	New: func() any { return new(request) },
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.ptrIdxs = req.ptrIdxs[:0]
	req.ptrBuckets = req.ptrBuckets[:0]
	req.Err = nil
	req.RefCount.Reset()
	req.enqueueAt = time.Time{}
	req.wg = sync.WaitGroup{}
}


func (req *request) loadEntries(entries []*kv.Entry) {
	if cap(req.Entries) < len(entries) {
		req.Entries = make([]*kv.Entry, len(entries))
	} else {
		req.Entries = req.Entries[:len(entries)]
	}
	copy(req.Entries, entries)
}

// IncrRef adds one lifecycle reference.
func (req *request) IncrRef() { req.Incr() }

// DecrRef releases one lifecycle reference and returns the request to pool at zero.
// It panics on refcount underflow to surface lifecycle bugs early.
func (req *request) DecrRef() {
	if req.Decr() > 0 {
		return
	}
	// ref == 0: last reference removed, release entries and return to pool.
	for _, e := range req.Entries {
		e.DecrRef()
	}
	req.Entries = nil
	req.Ptrs = nil
	req.ptrIdxs = nil
	req.ptrBuckets = nil
	requestPool.Put(req)
}

// Wait blocks until commit workers finish processing this request.
func (req *request) Wait() error {
	req.wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}
