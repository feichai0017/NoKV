// Package dbcore holds the shared write-envelope (Request) plus the
// generic lifecycle/policy helpers consumed by the commit pipeline
// (dbcore/commit) and the DB facade (root NoKV).
package dbcore

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

// Request is the dbcore write-envelope used by the DB write pipeline
// It is intentionally internal to the repository: callers should interact with
// DB APIs instead of constructing write-pipeline requests.
//
// All higher-level commit-pipeline types — CommitRequest, CommitQueue,
// CommitBatch, SyncBatch — live in dbcore/commit alongside the
// Pipeline that owns them.
type Request struct {
	Entries []*kv.Entry
	Err     error
	utils.RefCount
	EnqueueAt time.Time
	WG        sync.WaitGroup
}

// RequestPool reuses write-envelope objects on the DB commit hot path.
var RequestPool = sync.Pool{
	New: func() any { return new(Request) },
}

func (req *Request) Reset() {
	req.Entries = req.Entries[:0]
	req.Err = nil
	req.RefCount.Reset()
	req.EnqueueAt = time.Time{}
	req.WG = sync.WaitGroup{}
}

func (req *Request) LoadEntries(entries []*kv.Entry) {
	if cap(req.Entries) < len(entries) {
		req.Entries = make([]*kv.Entry, len(entries))
	} else {
		req.Entries = req.Entries[:len(entries)]
	}
	copy(req.Entries, entries)
}

// IncrRef adds one lifecycle reference.
func (req *Request) IncrRef() { req.Incr() }

// DecrRef releases one lifecycle reference and returns the request to
// pool at zero. It panics on refcount underflow to surface lifecycle
// bugs early.
func (req *Request) DecrRef() {
	if req.Decr() > 0 {
		return
	}
	for _, e := range req.Entries {
		e.DecrRef()
	}
	req.Entries = nil
	RequestPool.Put(req)
}

// Wait blocks until commit workers finish processing this request.
func (req *Request) Wait() error {
	req.WG.Wait()
	err := req.Err
	req.DecrRef()
	return err
}
