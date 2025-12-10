package NoKV

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
)

const (
	defaultWriteBatchMaxCount = 64
	defaultWriteBatchMaxSize  = 1 << 20
)

var (
	errWriteThrottleEnabled = errors.New("write throttle enabled due to L0 backlog")
	errWriteThrottleRelease = errors.New("write throttle released")
)

var commitReqPool = sync.Pool{
	New: func() any { return &commitRequest{} },
}

func (db *DB) initWriteBatchOptions() {
	if db.opt.WriteBatchMaxCount <= 0 {
		db.opt.WriteBatchMaxCount = defaultWriteBatchMaxCount
	}
	if db.opt.WriteBatchMaxSize <= 0 {
		db.opt.WriteBatchMaxSize = defaultWriteBatchMaxSize
	}
}

func (db *DB) applyThrottle(enable bool) {
	var val int32
	if enable {
		val = 1
	}
	prev := atomic.SwapInt32(&db.blockWrites, val)
	if prev == val {
		return
	}
	if enable {
		utils.Err(errWriteThrottleEnabled)
	} else {
		utils.Err(errWriteThrottleRelease)
	}
}

func (db *DB) sendToWriteCh(entries []*kv.Entry) (*request, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return nil, utils.ErrBlockedWrites
	}
	var size int64
	count := int64(len(entries))
	for _, e := range entries {
		size += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
	}
	if count >= db.opt.MaxBatchCount || size >= db.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
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
	cr.hot = db.isHotWrite(entries)

	if err := db.enqueueCommitRequest(cr); err != nil {
		req.wg.Done()
		req.DecrRef()
		commitReqPool.Put(cr)
		return nil, err
	}

	return req, nil
}

// Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*kv.Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

func (db *DB) enqueueCommitRequest(cr *commitRequest) error {
	if cr == nil {
		return nil
	}
	if atomic.LoadUint32(&db.commitQueue.closed) == 1 {
		return utils.ErrBlockedWrites
	}
	cq := &db.commitQueue

	if cq.ring == nil {
		return utils.ErrBlockedWrites
	}

	atomic.AddInt64(&cq.pendingEntries, int64(cr.entryCount))
	atomic.AddInt64(&cq.pendingBytes, cr.size)

	// Lazy init conds in case tests construct DB without running Open path.
	if cq.notEmpty == nil || cq.notFull == nil {
		cq.notEmpty = sync.NewCond(&cq.mu)
		cq.notFull = sync.NewCond(&cq.mu)
	}

	cq.mu.Lock()
	for {
		if atomic.LoadUint32(&cq.closed) == 1 {
			cq.mu.Unlock()
			return utils.ErrBlockedWrites
		}
		if cq.ring.Push(cr) {
			qLen := cq.ring.Len()
			qEntries := atomic.LoadInt64(&cq.pendingEntries)
			qBytes := atomic.LoadInt64(&cq.pendingBytes)
			cq.notEmpty.Signal()
			cq.mu.Unlock()
			db.writeMetrics.updateQueue(qLen, int(qEntries), qBytes)
			return nil
		}
		cq.notFull.Wait()
	}
}

func (db *DB) nextCommitBatch() []*commitRequest {
	cq := &db.commitQueue
	cq.mu.Lock()
	for cq.ring.Len() == 0 && atomic.LoadUint32(&cq.closed) == 0 {
		cq.notEmpty.Wait()
	}
	if cq.ring.Len() == 0 && atomic.LoadUint32(&cq.closed) == 1 {
		cq.mu.Unlock()
		return nil
	}

	batch := db.commitBatchPool.Get().([]*commitRequest)
	batch = batch[:0]
	pendingEntries := int64(0)
	pendingBytes := int64(0)

	// Adapt batch size to current backlog to drain the queue faster under load
	// and reduce wake/sleep churn on the condition variable. Caps keep the batch
	// from growing without bound to avoid long pauses.
	limitCount := db.opt.WriteBatchMaxCount
	limitSize := db.opt.WriteBatchMaxSize
	backlog := cq.ring.Len()
	if backlog > limitCount && limitCount > 0 {
		factor := min(max(backlog/limitCount, 1), 4)
		if scaled := limitCount * factor; scaled > 0 {
			limitCount = min(scaled, backlog)
		}
		if scaled := limitSize * int64(factor); scaled > 0 {
			limitSize = scaled
		}
	}

	popOne := func() bool {
		if cr, ok := cq.ring.Pop(); ok {
			batch = append(batch, cr)
			pendingEntries += int64(cr.entryCount)
			pendingBytes += cr.size
			if cr.hot {
				mult := db.opt.HotWriteBatchMultiplier
				if mult <= 0 {
					mult = 2
				}
				if mult > 4 {
					mult = 4
				}
				limitCount = min(limitCount*mult, db.opt.WriteBatchMaxCount*mult)
				if scaled := limitSize * int64(mult); scaled > 0 {
					limitSize = scaled
				}
			}
			return true
		}
		return false
	}

	popOne()
	for len(batch) < limitCount && pendingBytes < limitSize {
		if !popOne() {
			break
		}
	}

	atomic.AddInt64(&cq.pendingEntries, -pendingEntries)
	atomic.AddInt64(&cq.pendingBytes, -pendingBytes)
	qLen := cq.ring.Len()
	qEntries := atomic.LoadInt64(&cq.pendingEntries)
	qBytes := atomic.LoadInt64(&cq.pendingBytes)
	cq.notFull.Broadcast()
	cq.mu.Unlock()
	db.writeMetrics.updateQueue(qLen, int(qEntries), qBytes)
	return batch
}

func (db *DB) commitWorker() {
	defer db.commitWG.Done()
	for {
		reqs := db.nextCommitBatch()
		if reqs == nil {
			return
		}
		db.handleCommitRequests(reqs)
		db.releaseCommitBatch(reqs)
	}
}

func (db *DB) stopCommitWorkers() {
	if atomic.CompareAndSwapUint32(&db.commitQueue.closed, 0, 1) {
		db.commitQueue.mu.Lock()
		db.commitQueue.notEmpty.Broadcast()
		db.commitQueue.notFull.Broadcast()
		db.commitQueue.mu.Unlock()
	}
	db.commitWG.Wait()
}

func (db *DB) handleCommitRequests(reqs []*commitRequest) {
	if len(reqs) == 0 {
		return
	}

	requests := make([]*request, 0, len(reqs))
	var (
		totalEntries int
		totalSize    int64
		waitSum      int64
	)
	batchStart := time.Now()
	now := batchStart
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

	if len(requests) == 0 {
		db.finishCommitRequests(reqs, nil)
		return
	}

	if db.writeMetrics != nil {
		db.writeMetrics.recordBatch(len(requests), totalEntries, totalSize, waitSum)
	}

	if db.vwriter != nil {
		if err := db.vwriter.WriteRequests(requests); err != nil {
			db.finishCommitRequests(reqs, err)
			return
		}
	} else if err := db.vlog.write(requests); err != nil {
		db.finishCommitRequests(reqs, err)
		return
	}
	var valueLogDur time.Duration
	if db.writeMetrics != nil {
		valueLogDur = max(time.Since(batchStart), 0)
		if valueLogDur > 0 {
			db.writeMetrics.recordValueLog(valueLogDur)
		}
	}

	if err := db.applyRequests(requests); err != nil {
		db.finishCommitRequests(reqs, err)
		return
	}
	if db.writeMetrics != nil {
		totalDur := max(time.Since(batchStart), 0)
		applyDur := max(totalDur-valueLogDur, 0)
		if applyDur > 0 {
			db.writeMetrics.recordApply(applyDur)
		}
	}

	if db.opt.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			db.finishCommitRequests(reqs, err)
			return
		}
	}

	db.finishCommitRequests(reqs, nil)
}

func (db *DB) releaseCommitBatch(batch []*commitRequest) {
	if batch == nil {
		return
	}
	for i := range batch {
		batch[i] = nil
	}
	db.commitBatchPool.Put(batch[:0])
}

func (db *DB) applyRequests(reqs []*request) error {
	for _, r := range reqs {
		if r == nil || len(r.Entries) == 0 {
			continue
		}
		if err := db.writeToLSM(r); err != nil {
			return pkgerrors.Wrap(err, "writeRequests")
		}
		db.Lock()
		db.updateHead(r.Ptrs)
		db.Unlock()
	}
	return nil
}

func (db *DB) finishCommitRequests(reqs []*commitRequest, err error) {
	for _, cr := range reqs {
		if cr == nil || cr.req == nil {
			continue
		}
		cr.req.Err = err
		cr.req.wg.Done()
		cr.req = nil
		cr.entryCount = 0
		cr.size = 0
		commitReqPool.Put(cr)
	}
}

func (db *DB) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		if db.shouldWriteValueToLSM(entry) { // Will include deletion / tombstone case.
			entry.Meta = entry.Meta &^ kv.BitValuePointer
		} else {
			entry.Meta = entry.Meta | kv.BitValuePointer
			entry.Value = b.Ptrs[i].Encode()
		}
		db.lsm.Set(entry)
		db.recordCFWrite(entry.CF, 1)
	}
	return nil
}
