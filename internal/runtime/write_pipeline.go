package runtime

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

// Request is the runtime write-envelope used by the DB write pipeline and the
// value-log path. It is intentionally internal to the repository: callers
// should interact with DB APIs instead of constructing write-pipeline requests.
type Request struct {
	Entries    []*kv.Entry
	Ptrs       []kv.ValuePtr
	PtrIdxs    []int
	PtrBuckets []uint32
	Err        error
	utils.RefCount
	EnqueueAt time.Time
	WG        sync.WaitGroup
}

// RequestPool reuses write-envelope objects on the DB/value-log hot path.
var RequestPool = sync.Pool{
	New: func() any { return new(Request) },
}

func (req *Request) Reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.PtrIdxs = req.PtrIdxs[:0]
	req.PtrBuckets = req.PtrBuckets[:0]
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

// DecrRef releases one lifecycle reference and returns the request to pool at
// zero. It panics on refcount underflow to surface lifecycle bugs early.
func (req *Request) DecrRef() {
	if req.Decr() > 0 {
		return
	}
	for _, e := range req.Entries {
		e.DecrRef()
	}
	req.Entries = nil
	req.Ptrs = nil
	req.PtrIdxs = nil
	req.PtrBuckets = nil
	RequestPool.Put(req)
}

// Wait blocks until commit workers finish processing this request.
func (req *Request) Wait() error {
	req.WG.Wait()
	err := req.Err
	req.DecrRef()
	return err
}

// CommitRequest is the queue element used by the DB commit worker.
type CommitRequest struct {
	Req        *Request
	EntryCount int
	Size       int64
}

// CommitRequestPool reuses commit-request envelopes on the write hot path.
var CommitRequestPool = sync.Pool{
	New: func() any { return &CommitRequest{} },
}

// CommitQueue is the MPSC-backed queue shared by write submitters and the
// commit worker.
type CommitQueue struct {
	q              *utils.MPSCQueue[*CommitRequest]
	pendingBytes   atomic.Int64
	pendingEntries atomic.Int64
}

func (cq *CommitQueue) Init(capacity int) {
	cq.q = utils.NewMPSCQueue[*CommitRequest](capacity)
}

func (cq *CommitQueue) Close() bool {
	if cq == nil || cq.q == nil {
		return false
	}
	return cq.q.Close()
}

func (cq *CommitQueue) Closed() bool {
	return cq == nil || cq.q == nil || cq.q.Closed()
}

func (cq *CommitQueue) CloseCh() <-chan struct{} {
	if cq == nil || cq.q == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return cq.q.CloseCh()
}

func (cq *CommitQueue) Len() int {
	if cq == nil || cq.q == nil {
		return 0
	}
	return cq.q.ReservedLen()
}

func (cq *CommitQueue) Consumer() *utils.MPSCConsumer[*CommitRequest] {
	if cq == nil || cq.q == nil {
		return nil
	}
	return cq.q.AcquireConsumer()
}

func (cq *CommitQueue) Push(cr *CommitRequest) bool {
	if cq == nil || cq.q == nil {
		return false
	}
	return cq.q.Push(cr)
}

func (cq *CommitQueue) Pop(c *utils.MPSCConsumer[*CommitRequest]) *CommitRequest {
	if cq == nil || cq.q == nil || c == nil {
		return nil
	}
	cr, ok := c.Pop()
	if !ok {
		return nil
	}
	return cr
}

func (cq *CommitQueue) DrainReady(c *utils.MPSCConsumer[*CommitRequest], max int, fn func(*CommitRequest) bool) int {
	if cq == nil || cq.q == nil || c == nil {
		return 0
	}
	return c.DrainReady(max, fn)
}

func (cq *CommitQueue) PendingEntries() int64 {
	if cq == nil {
		return 0
	}
	return cq.pendingEntries.Load()
}

func (cq *CommitQueue) PendingBytes() int64 {
	if cq == nil {
		return 0
	}
	return cq.pendingBytes.Load()
}

func (cq *CommitQueue) AddPending(entries int64, bytes int64) {
	if cq == nil {
		return
	}
	cq.pendingEntries.Add(entries)
	cq.pendingBytes.Add(bytes)
}

// CommitBatch is the temporary grouping drained by one commit-worker pass.
// ShardID is set by the dispatcher and pins the batch to one LSM data-plane
// shard end-to-end (preserves SetBatch atomicity).
type CommitBatch struct {
	Reqs        []*CommitRequest
	Pool        *[]*CommitRequest
	Requests    []*Request
	ShardID     int
	BatchStart  time.Time
	ValueLogDur time.Duration
}

// SyncBatch is the handoff object between the commit worker and the sync worker.
type SyncBatch struct {
	Reqs      []*CommitRequest
	Pool      *[]*CommitRequest
	Requests  []*Request
	ShardID   int
	FailedAt  int
	ApplyDone time.Time
}
