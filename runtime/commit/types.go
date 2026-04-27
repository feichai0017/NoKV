package commit

// Queue + batch + sync envelope types owned by the commit pipeline.
// Request (the underlying write envelope shared with engine/vlog) lives
// in runtime — see runtime/request.go.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/runtime"
	"github.com/feichai0017/NoKV/utils"
)

// CommitRequest is the queue element used by the commit Pipeline. It
// wraps a runtime.Request with the queue-side accounting (entry count,
// payload size) the dispatcher needs.
type CommitRequest struct {
	Req        *runtime.Request
	EntryCount int
	Size       int64
}

// CommitRequestPool reuses commit-request envelopes on the write hot path.
var CommitRequestPool = sync.Pool{
	New: func() any { return &CommitRequest{} },
}

// CommitQueue is the MPSC-backed queue shared by write submitters and
// the commit dispatcher.
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

// CommitBatch is the temporary grouping drained by one commit-worker
// pass. ShardID is set by the dispatcher and pins the batch to one LSM
// data-plane shard end-to-end (preserves SetBatch atomicity).
type CommitBatch struct {
	Reqs        []*CommitRequest
	Pool        *[]*CommitRequest
	Requests    []*runtime.Request
	ShardID     int
	BatchStart  time.Time
	ValueLogDur time.Duration
}

// SyncBatch is the handoff object between the commit worker and the
// sync worker.
type SyncBatch struct {
	Reqs      []*CommitRequest
	Pool      *[]*CommitRequest
	Requests  []*runtime.Request
	ShardID   int
	FailedAt  int
	ApplyDone time.Time
}
