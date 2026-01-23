package NoKV

import (
	"errors"
	"runtime"
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
	if !atomic.CompareAndSwapUint32(&cq.closed, 0, 1) {
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
		if atomic.LoadUint32(&cq.closed) == 1 {
			if atomic.LoadInt64(&cq.queueLen) == 0 && atomic.LoadInt64(&cq.inflight) == 0 {
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
			atomic.AddInt64(&cq.queueLen, -1)
			cq.releaseSpace()
			return cr
		}
		if atomic.LoadUint32(&cq.closed) == 1 && atomic.LoadInt64(&cq.queueLen) == 0 {
			return nil
		}
		runtime.Gosched()
	}
}

func (db *DB) initWriteBatchOptions() {
	if db.opt.WriteBatchMaxCount <= 0 {
		db.opt.WriteBatchMaxCount = defaultWriteBatchMaxCount
	}
	if db.opt.WriteBatchMaxSize <= 0 {
		db.opt.WriteBatchMaxSize = defaultWriteBatchMaxSize
	}
	if db.opt.MaxBatchCount <= 0 {
		db.opt.MaxBatchCount = int64(db.opt.WriteBatchMaxCount)
	}
	if db.opt.MaxBatchSize <= 0 {
		db.opt.MaxBatchSize = db.opt.WriteBatchMaxSize
	}
	if db.opt.WriteBatchWait < 0 {
		db.opt.WriteBatchWait = 0
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
		_ = utils.Err(errWriteThrottleEnabled)
	} else {
		_ = utils.Err(errWriteThrottleRelease)
	}
}

func (db *DB) sendToWriteCh(entries []*kv.Entry, waitOnThrottle bool) (*request, error) {
	for atomic.LoadInt32(&db.blockWrites) == 1 {
		if !waitOnThrottle {
			return nil, utils.ErrBlockedWrites
		}
		if atomic.LoadUint32(&db.isClosed) == 1 || atomic.LoadUint32(&db.commitQueue.closed) == 1 {
			return nil, utils.ErrBlockedWrites
		}
		time.Sleep(200 * time.Microsecond)
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
	req, err := db.sendToWriteCh(entries, true)
	if err != nil {
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

	atomic.AddInt64(&cq.inflight, 1)
	defer atomic.AddInt64(&cq.inflight, -1)
	if atomic.LoadUint32(&cq.closed) == 1 {
		return utils.ErrBlockedWrites
	}

	atomic.AddInt64(&cq.pendingEntries, int64(cr.entryCount))
	atomic.AddInt64(&cq.pendingBytes, cr.size)
	queued := false
	defer func() {
		if !queued {
			atomic.AddInt64(&cq.pendingEntries, -int64(cr.entryCount))
			atomic.AddInt64(&cq.pendingBytes, -cr.size)
		}
	}()

	if !cq.acquireSpace() {
		return utils.ErrBlockedWrites
	}
	if atomic.LoadUint32(&cq.closed) == 1 {
		cq.releaseSpace()
		return utils.ErrBlockedWrites
	}
	if !cq.ring.Push(cr) {
		cq.releaseSpace()
		return utils.ErrBlockedWrites
	}
	atomic.AddInt64(&cq.queueLen, 1)
	cq.releaseItem()
	queued = true

	qLen := int(atomic.LoadInt64(&cq.queueLen))
	qEntries := atomic.LoadInt64(&cq.pendingEntries)
	qBytes := atomic.LoadInt64(&cq.pendingBytes)
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
	backlog := int(atomic.LoadInt64(&cq.queueLen))
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
	}

	if cr := cq.pop(); cr != nil {
		addToBatch(cr)
	}
	if coalesceWait > 0 && atomic.LoadInt64(&cq.queueLen) == 0 && len(batch) < limitCount && pendingBytes < limitSize {
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

	atomic.AddInt64(&cq.pendingEntries, -pendingEntries)
	atomic.AddInt64(&cq.pendingBytes, -pendingBytes)
	qLen := int(atomic.LoadInt64(&cq.queueLen))
	qEntries := atomic.LoadInt64(&cq.pendingEntries)
	qBytes := atomic.LoadInt64(&cq.pendingBytes)
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
			db.finishCommitRequests(batch.reqs, nil)
			db.releaseCommitBatch(batch)
			continue
		}
		batch.requests = requests
		if db.writeMetrics != nil {
			db.writeMetrics.RecordBatch(len(requests), totalEntries, totalSize, waitSum)
		}

		err := db.vlog.write(requests)

		if err != nil {
			db.finishCommitRequests(batch.reqs, err)
			db.releaseCommitBatch(batch)
			continue
		}
		if db.writeMetrics != nil {
			batch.valueLogDur = max(time.Since(batch.batchStart), 0)
			if batch.valueLogDur > 0 {
				db.writeMetrics.RecordValueLog(batch.valueLogDur)
			}
		}

		err = db.applyRequests(batch.requests)
		if err == nil && db.opt.SyncWrites {
			err = db.wal.Sync()
		}
		if db.writeMetrics != nil {
			totalDur := max(time.Since(batch.batchStart), 0)
			applyDur := max(totalDur-batch.valueLogDur, 0)
			if applyDur > 0 {
				db.writeMetrics.RecordApply(applyDur)
			}
		}
		db.finishCommitRequests(batch.reqs, err)
		db.releaseCommitBatch(batch)
	}
}

func (db *DB) stopCommitWorkers() {
	db.commitQueue.close()
	db.commitWG.Wait()
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
		db.recordCFWrite(entry.CF, 1)
	}
	if err := db.lsm.SetBatch(b.Entries); err != nil {
		return err
	}
	return nil
}
