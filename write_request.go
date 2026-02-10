package NoKV

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
)

type request struct {
	Entries   []*kv.Entry
	Ptrs      []kv.ValuePtr
	Err       error
	ref       int32
	enqueueAt time.Time
	wg        sync.WaitGroup
}

var requestPool = sync.Pool{
	New: func() any { return new(request) },
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.Err = nil
	req.ref = 0
	req.enqueueAt = time.Time{}
	req.wg = sync.WaitGroup{}
}

// IncrRef is part of the exported receiver API.
func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) loadEntries(entries []*kv.Entry) {
	if cap(req.Entries) < len(entries) {
		req.Entries = make([]*kv.Entry, len(entries))
	} else {
		req.Entries = req.Entries[:len(entries)]
	}
	copy(req.Entries, entries)
}

// DecrRef is part of the exported receiver API.
func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	for _, e := range req.Entries {
		e.DecrRef()
	}
	req.Entries = nil
	req.Ptrs = nil
	requestPool.Put(req)
}

// Wait is part of the exported receiver API.
func (req *request) Wait() error {
	req.wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}
