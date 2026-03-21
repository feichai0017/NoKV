package NoKV

import (
	"log/slog"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
)

var commitReqPool = sync.Pool{
	New: func() any { return &commitRequest{} },
}

func (cq *commitQueue) init(capacity int) {
	if capacity < 2 {
		capacity = 2
	}
	cq.ring = utils.NewRing[*commitRequest](capacity)
	cap := cq.ring.Cap()
	cq.items = make(chan struct{}, cap)
	cq.spaces = make(chan struct{}, cap)
	cq.closeCh = make(chan struct{})
	for range cap {
		cq.spaces <- struct{}{}
	}
}

func (cq *commitQueue) close() bool {
	if cq == nil {
		return false
	}
	if !cq.closed.CompareAndSwap(0, 1) {
		return false
	}
	if cq.ring != nil {
		cq.ring.Close()
	}
	if cq.closeCh != nil {
		close(cq.closeCh)
	}
	return true
}

func (cq *commitQueue) acquireSpace() bool {
	for {
		select {
		case <-cq.spaces:
			return true
		case <-cq.closeCh:
			return false
		}
	}
}

func (cq *commitQueue) releaseSpace() {
	cq.spaces <- struct{}{}
}

func (cq *commitQueue) releaseItem() {
	cq.items <- struct{}{}
}

func (cq *commitQueue) tryAcquireItem() bool {
	select {
	case <-cq.items:
		return true
	default:
		return false
	}
}

func (cq *commitQueue) acquireItem() bool {
	for {
		if cq.tryAcquireItem() {
			return true
		}
		if cq.closed.Load() == 1 {
			if cq.queueLen.Load() == 0 && cq.inflight.Load() == 0 {
				return false
			}
			time.Sleep(100 * time.Microsecond)
			continue
		}
		select {
		case <-cq.items:
			return true
		case <-cq.closeCh:
		}
	}
}

func (cq *commitQueue) pop() *commitRequest {
	for {
		if cr, ok := cq.ring.Pop(); ok {
			cq.queueLen.Add(-1)
			cq.releaseSpace()
			return cr
		}
		if cq.closed.Load() == 1 && cq.queueLen.Load() == 0 {
			return nil
		}
		runtime.Gosched()
	}
}

func (db *DB) applyThrottle(state lsm.WriteThrottleState) {
	state = normalizeWriteThrottleState(state)
	stop := int32(0)
	slow := int32(0)
	switch state {
	case lsm.WriteThrottleStop:
		stop = 1
	case lsm.WriteThrottleSlowdown:
		slow = 1
	}
	prevStop := db.blockWrites.Swap(stop)
	prevSlow := db.slowWrites.Swap(slow)
	if prevStop == stop && prevSlow == slow {
		return
	}
	switch state {
	case lsm.WriteThrottleStop:
		slog.Default().Warn("write stop enabled due to compaction backlog")
	case lsm.WriteThrottleSlowdown:
		slog.Default().Info("write slowdown enabled due to compaction backlog")
	default:
		slog.Default().Info("write throttling cleared")
	}
}

func (db *DB) sendToWriteCh(entries []*kv.Entry, waitOnThrottle bool) (*request, error) {
	var size int64
	count := int64(len(entries))
	for _, e := range entries {
		size += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
	}
	if count >= db.opt.MaxBatchCount || size >= db.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}
	if db.slowWrites.Load() == 1 {
		if d := db.currentSlowdownDelay(size); d > 0 {
			time.Sleep(d)
		}
	}
	for db.blockWrites.Load() == 1 {
		if !waitOnThrottle {
			return nil, utils.ErrBlockedWrites
		}
		if db.isClosed.Load() == 1 || db.commitQueue.closed.Load() == 1 {
			return nil, utils.ErrBlockedWrites
		}
		time.Sleep(200 * time.Microsecond)
	}

	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	if db.writeMetrics != nil {
		req.enqueueAt = time.Now()
	}
	req.wg.Add(1)
	req.IncrRef() // for db write

	cr := commitReqPool.Get().(*commitRequest)
	cr.req = req
	cr.entryCount = int(count)
	cr.size = size

	if err := db.enqueueCommitRequest(cr); err != nil {
		req.wg.Done()
		// Keep entry ownership with batchSet error handling; request cleanup here
		// should not release caller-provided entries.
		req.Entries = nil
		req.DecrRef()
		commitReqPool.Put(cr)
		return nil, err
	}

	return req, nil
}

func normalizeWriteThrottleState(state lsm.WriteThrottleState) lsm.WriteThrottleState {
	switch state {
	case lsm.WriteThrottleNone, lsm.WriteThrottleSlowdown, lsm.WriteThrottleStop:
		return state
	default:
		return lsm.WriteThrottleNone
	}
}

func (db *DB) currentSlowdownDelay(batchSize int64) time.Duration {
	if batchSize <= 0 || db.lsm == nil {
		return 0
	}
	rate := db.lsm.ThrottleRateBytesPerSec()
	if rate == 0 {
		return 0
	}
	delayNs := (uint64(batchSize) * uint64(time.Second)) / rate
	if delayNs == 0 {
		return 0
	}
	if delayNs > uint64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(delayNs)
}

func (db *DB) batchSet(entries []*kv.Entry) error {
	req, err := db.sendToWriteCh(entries, true)
	if err != nil {
		// batchSet consumes one entry reference regardless of queueing outcome.
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
		return err
	}

	return req.Wait()
}

func (db *DB) enqueueCommitRequest(cr *commitRequest) error {
	if cr == nil {
		return nil
	}
	cq := &db.commitQueue

	if cq.ring == nil || cq.items == nil || cq.spaces == nil {
		return utils.ErrBlockedWrites
	}

	cq.inflight.Add(1)
	defer cq.inflight.Add(-1)
	if cq.closed.Load() == 1 {
		return utils.ErrBlockedWrites
	}

	cq.pendingEntries.Add(int64(cr.entryCount))
	cq.pendingBytes.Add(cr.size)
	queued := false
	defer func() {
		if !queued {
			cq.pendingEntries.Add(-int64(cr.entryCount))
			cq.pendingBytes.Add(-cr.size)
		}
	}()

	if !cq.acquireSpace() {
		return utils.ErrBlockedWrites
	}
	if cq.closed.Load() == 1 {
		cq.releaseSpace()
		return utils.ErrBlockedWrites
	}
	if !cq.ring.Push(cr) {
		cq.releaseSpace()
		return utils.ErrBlockedWrites
	}
	cq.queueLen.Add(1)
	cq.releaseItem()
	queued = true

	qLen := int(cq.queueLen.Load())
	qEntries := cq.pendingEntries.Load()
	qBytes := cq.pendingBytes.Load()
	db.writeMetrics.UpdateQueue(qLen, int(qEntries), qBytes)
	return nil
}

func (db *DB) nextCommitBatch() *commitBatch {
	cq := &db.commitQueue
	if !cq.acquireItem() {
		return nil
	}

	batchPtr := db.commitBatchPool.Get().(*[]*commitRequest)
	batch := (*batchPtr)[:0]
	pendingEntries := int64(0)
	pendingBytes := int64(0)
	coalesceWait := db.opt.WriteBatchWait

	// Adapt batch size to current backlog to drain the queue faster under load
	// and reduce wake/sleep churn on the condition variable. Caps keep the batch
	// from growing without bound to avoid long pauses.
	limitCount := db.opt.WriteBatchMaxCount
	limitSize := db.opt.WriteBatchMaxSize
	backlog := int(cq.queueLen.Load())
	if backlog > limitCount && limitCount > 0 {
		factor := min(max(backlog/limitCount, 1), 4)
		if scaled := limitCount * factor; scaled > 0 {
			limitCount = min(scaled, backlog)
		}
		if scaled := limitSize * int64(factor); scaled > 0 {
			limitSize = scaled
		}
	}

	addToBatch := func(cr *commitRequest) {
		batch = append(batch, cr)
		pendingEntries += int64(cr.entryCount)
		pendingBytes += cr.size
	}

	if cr := cq.pop(); cr != nil {
		addToBatch(cr)
	}
	if coalesceWait > 0 && cq.queueLen.Load() == 0 && len(batch) < limitCount && pendingBytes < limitSize {
		// Allow a brief coalescing window when the queue is momentarily empty.
		time.Sleep(coalesceWait)
	}
	for len(batch) < limitCount && pendingBytes < limitSize {
		if !cq.tryAcquireItem() {
			break
		}
		if cr := cq.pop(); cr != nil {
			addToBatch(cr)
		}
	}

	cq.pendingEntries.Add(-pendingEntries)
	cq.pendingBytes.Add(-pendingBytes)
	qLen := int(cq.queueLen.Load())
	qEntries := cq.pendingEntries.Load()
	qBytes := cq.pendingBytes.Load()
	db.writeMetrics.UpdateQueue(qLen, int(qEntries), qBytes)
	return &commitBatch{reqs: batch, pool: batchPtr}
}

func (db *DB) commitWorker() {
	defer db.commitWG.Done()
	for {
		batch := db.nextCommitBatch()
		if batch == nil {
			return
		}
		batch.batchStart = time.Now()
		requests, totalEntries, totalSize, waitSum := db.collectCommitRequests(batch.reqs, batch.batchStart)
		if len(requests) == 0 {
			db.ackCommitBatch(batch.reqs, batch.pool, nil, -1, nil)
			continue
		}
		batch.requests = requests
		if db.writeMetrics != nil {
			db.writeMetrics.RecordBatch(len(requests), totalEntries, totalSize, waitSum)
		}

		err := db.vlog.write(requests)

		if err != nil {
			db.ackCommitBatch(batch.reqs, batch.pool, requests, -1, err)
			continue
		}
		if db.writeMetrics != nil {
			batch.valueLogDur = max(time.Since(batch.batchStart), 0)
			if batch.valueLogDur > 0 {
				db.writeMetrics.RecordValueLog(batch.valueLogDur)
			}
		}

		failedAt, err := db.applyRequests(batch.requests)

		// If a dedicated sync pipeline is enabled and apply succeeded, hand off
		// to the sync worker for fsync + ack so we don't block the commit loop.
		if err == nil && db.syncQueue != nil {
			sb := &syncBatch{
				reqs:      batch.reqs,
				pool:      batch.pool,
				requests:  batch.requests,
				failedAt:  failedAt,
				applyDone: time.Now(),
			}
			// Detach from commitBatch so releaseCommitBatch won't reclaim it.
			batch.reqs = nil
			batch.pool = nil
			db.releaseCommitBatch(batch)
			db.syncQueue <- sb
			continue
		}

		if err == nil && db.opt.SyncWrites {
			syncStart := time.Now()
			err = db.wal.Sync()
			if db.writeMetrics != nil {
				db.writeMetrics.RecordSync(time.Since(syncStart), 1)
			}
		}

		// Record apply metrics.
		if db.writeMetrics != nil {
			totalDur := max(time.Since(batch.batchStart), 0)
			applyDur := max(totalDur-batch.valueLogDur, 0)
			if applyDur > 0 {
				db.writeMetrics.RecordApply(applyDur)
			}
		}

		db.ackCommitBatch(batch.reqs, batch.pool, batch.requests, failedAt, err)
	}
}

// syncWorker runs a dedicated goroutine that batches pending syncBatch items,
// calls wal.Sync() once for the whole batch, then acks all enclosed requests.
// This decouples the fsync latency from the commit pipeline so commitWorker can
// keep applying new writes to the LSM/WAL buffer while a previous fsync is in
// flight.
func (db *DB) syncWorker() {
	defer db.syncWG.Done()

	// Temporary buffer for draining the channel in bulk.
	pending := make([]*syncBatch, 0, 64)

	for first := range db.syncQueue {
		pending = append(pending, first)
		// Drain everything currently queued so we coalesce a single fsync.
	drain:
		for {
			select {
			case sb, ok := <-db.syncQueue:
				if !ok {
					break drain
				}
				pending = append(pending, sb)
			default:
				break drain
			}
		}

		// One fsync covers all pending batches.
		syncStart := time.Now()
		syncErr := db.wal.Sync()
		if db.writeMetrics != nil {
			db.writeMetrics.RecordSync(time.Since(syncStart), len(pending))
		}

		// Ack every request in every pending syncBatch.
		for _, sb := range pending {
			db.ackCommitBatch(sb.reqs, sb.pool, sb.requests, sb.failedAt, syncErr)
		}
		pending = pending[:0]
	}
}

// ackCommitBatch finishes a batch of commit requests: sets per-request errors,
// signals waiters, and returns the backing slice to the pool.
//
// requests is the ordered slice returned by collectCommitRequests (same order as
// applyRequests processes them). failedAt is the index into requests where the
// first error occurred; requests before that index succeeded and get nil error.
// When failedAt < 0, every request receives defaultErr uniformly (which may be
// nil on success).
func (db *DB) ackCommitBatch(reqs []*commitRequest, pool *[]*commitRequest, requests []*request, failedAt int, defaultErr error) {
	if defaultErr != nil && failedAt >= 0 && failedAt < len(requests) {
		// Partial failure: only requests[failedAt:] receive the error;
		// earlier requests already applied successfully and get nil.
		perReqErr := make(map[*request]error, len(requests)-failedAt)
		for i := failedAt; i < len(requests); i++ {
			if requests[i] != nil {
				perReqErr[requests[i]] = defaultErr
			}
		}
		db.finishCommitRequests(reqs, nil, perReqErr)
	} else {
		// Global success or global failure — every request gets defaultErr.
		db.finishCommitRequests(reqs, defaultErr, nil)
	}
	if pool != nil {
		for i := range reqs {
			reqs[i] = nil
		}
		*pool = reqs[:0]
		db.commitBatchPool.Put(pool)
	}
}

func (db *DB) stopCommitWorkers() {
	db.commitQueue.close()
	db.commitWG.Wait()
	// After commit workers are done, close the sync queue so the sync worker
	// drains remaining batches and exits.
	if db.syncQueue != nil {
		close(db.syncQueue)
		db.syncWG.Wait()
	}
}

func (db *DB) collectCommitRequests(reqs []*commitRequest, now time.Time) ([]*request, int, int64, int64) {
	requests := make([]*request, 0, len(reqs))
	var (
		totalEntries int
		totalSize    int64
		waitSum      int64
	)
	for _, cr := range reqs {
		if cr == nil || cr.req == nil {
			continue
		}
		r := cr.req
		requests = append(requests, r)
		totalEntries += len(r.Entries)
		totalSize += cr.size
		if !r.enqueueAt.IsZero() {
			waitSum += now.Sub(r.enqueueAt).Nanoseconds()
			r.enqueueAt = time.Time{}
		}
	}
	return requests, totalEntries, totalSize, waitSum
}

func (db *DB) releaseCommitBatch(batch *commitBatch) {
	if batch == nil || batch.pool == nil {
		return
	}
	batch.requests = nil
	batch.batchStart = time.Time{}
	batch.valueLogDur = 0
	reqs := batch.reqs
	for i := range reqs {
		reqs[i] = nil
	}
	*batch.pool = reqs[:0]
	db.commitBatchPool.Put(batch.pool)
}

func (db *DB) applyRequests(reqs []*request) (int, error) {
	for i, r := range reqs {
		if r == nil || len(r.Entries) == 0 {
			continue
		}
		if err := db.writeToLSM(r); err != nil {
			return i, pkgerrors.Wrap(err, "writeRequests")
		}
		if len(r.ptrBuckets) == 0 {
			continue
		}
		db.Lock()
		db.updateHeadBuckets(r.ptrBuckets)
		db.Unlock()
	}
	return -1, nil
}

func (db *DB) finishCommitRequests(reqs []*commitRequest, defaultErr error, perReqErr map[*request]error) {
	for _, cr := range reqs {
		if cr == nil || cr.req == nil {
			continue
		}
		if perReqErr != nil {
			if reqErr, ok := perReqErr[cr.req]; ok {
				cr.req.Err = reqErr
			} else {
				cr.req.Err = defaultErr
			}
		} else {
			cr.req.Err = defaultErr
		}
		cr.req.wg.Done()
		cr.req = nil
		cr.entryCount = 0
		cr.size = 0
		commitReqPool.Put(cr)
	}
}

func (db *DB) writeToLSM(b *request) error {
	if len(b.ptrIdxs) == 0 {
		if len(b.Ptrs) != 0 && len(b.Ptrs) != len(b.Entries) {
			return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
		}
		return db.lsm.SetBatch(b.Entries)
	}
	if len(b.Ptrs) != len(b.Entries) {
		return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for _, idx := range b.ptrIdxs {
		entry := b.Entries[idx]
		entry.Meta = entry.Meta | kv.BitValuePointer
		entry.Value = b.Ptrs[idx].Encode()
	}
	if err := db.lsm.SetBatch(b.Entries); err != nil {
		return err
	}
	return nil
}
