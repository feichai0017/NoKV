package lsm

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/metrics"
)

type flushTask struct {
	memTable   *memTable
	queuedAt   time.Time
	buildStart time.Time
	installAt  time.Time
}

// flushRuntime is the concrete flush queue owned by LSM.
//
// The old flush.Manager abstraction only wrapped this one workflow while hiding
// it behind Stage/Data/Update machinery. Keep the queue concrete so the flush
// worker and its metrics can be reasoned about locally.
type flushRuntime struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	queue  []*flushTask

	pending       atomic.Int64
	queueLen      atomic.Int64
	activeCt      atomic.Int64
	waitSumNs     atomic.Int64
	waitCount     atomic.Int64
	waitLastNs    atomic.Int64
	waitMaxNs     atomic.Int64
	buildSumNs    atomic.Int64
	buildCount    atomic.Int64
	buildLastNs   atomic.Int64
	buildMaxNs    atomic.Int64
	releaseSumNs  atomic.Int64
	releaseCount  atomic.Int64
	releaseLastNs atomic.Int64
	releaseMaxNs  atomic.Int64
	completed     atomic.Int64
}

func newFlushRuntime() *flushRuntime {
	rt := &flushRuntime{
		queue: make([]*flushTask, 0),
	}
	rt.cond = sync.NewCond(&rt.mu)
	return rt
}

func (rt *flushRuntime) close() error {
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	rt.closed = true
	rt.mu.Unlock()
	rt.cond.Broadcast()
	return nil
}

func (rt *flushRuntime) enqueue(mt *memTable) error {
	if rt == nil {
		return ErrFlushRuntimeNil
	}
	if mt == nil {
		return ErrFlushRuntimeNilMemtable
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return ErrFlushRuntimeClosed
	}
	rt.queue = append(rt.queue, &flushTask{
		memTable: mt,
		queuedAt: time.Now(),
	})
	rt.pending.Add(1)
	rt.queueLen.Store(int64(len(rt.queue)))
	rt.cond.Signal()
	return nil
}

func (rt *flushRuntime) next() (*flushTask, bool) {
	if rt == nil {
		return nil, false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for !rt.closed && len(rt.queue) == 0 {
		rt.cond.Wait()
	}
	if len(rt.queue) == 0 {
		return nil, false
	}
	task := rt.queue[0]
	rt.queue = rt.queue[1:]
	rt.queueLen.Store(int64(len(rt.queue)))
	task.buildStart = time.Now()
	if !task.queuedAt.IsZero() {
		waitNs := time.Since(task.queuedAt).Nanoseconds()
		rt.waitSumNs.Add(waitNs)
		rt.waitCount.Add(1)
		rt.waitLastNs.Store(waitNs)
		updateMaxInt64(&rt.waitMaxNs, waitNs)
	}
	rt.activeCt.Add(1)
	return task, true
}

func (rt *flushRuntime) markInstalled(task *flushTask) {
	if rt == nil || task == nil {
		return
	}
	if !task.buildStart.IsZero() {
		buildNs := time.Since(task.buildStart).Nanoseconds()
		rt.buildSumNs.Add(buildNs)
		rt.buildCount.Add(1)
		rt.buildLastNs.Store(buildNs)
		updateMaxInt64(&rt.buildMaxNs, buildNs)
	}
	task.installAt = time.Now()
}

func (rt *flushRuntime) markDone(task *flushTask) {
	if rt == nil || task == nil {
		return
	}
	if !task.installAt.IsZero() {
		releaseNs := time.Since(task.installAt).Nanoseconds()
		rt.releaseSumNs.Add(releaseNs)
		rt.releaseCount.Add(1)
		rt.releaseLastNs.Store(releaseNs)
		updateMaxInt64(&rt.releaseMaxNs, releaseNs)
	}
	rt.activeCt.Add(-1)
	rt.pending.Add(-1)
	rt.completed.Add(1)
}

func (rt *flushRuntime) stats() metrics.FlushMetrics {
	if rt == nil {
		return metrics.FlushMetrics{}
	}
	return metrics.FlushMetrics{
		Pending:       rt.pending.Load(),
		Queue:         rt.queueLen.Load(),
		Active:        rt.activeCt.Load(),
		WaitNs:        rt.waitSumNs.Load(),
		WaitCount:     rt.waitCount.Load(),
		WaitLastNs:    rt.waitLastNs.Load(),
		WaitMaxNs:     rt.waitMaxNs.Load(),
		BuildNs:       rt.buildSumNs.Load(),
		BuildCount:    rt.buildCount.Load(),
		BuildLastNs:   rt.buildLastNs.Load(),
		BuildMaxNs:    rt.buildMaxNs.Load(),
		ReleaseNs:     rt.releaseSumNs.Load(),
		ReleaseCount:  rt.releaseCount.Load(),
		ReleaseLastNs: rt.releaseLastNs.Load(),
		ReleaseMaxNs:  rt.releaseMaxNs.Load(),
		Completed:     rt.completed.Load(),
	}
}

func updateMaxInt64(target *atomic.Int64, val int64) {
	for {
		current := target.Load()
		if val <= current {
			return
		}
		if target.CompareAndSwap(current, val) {
			return
		}
	}
}
