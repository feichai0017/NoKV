package lsm

import (
	"sync"

	"github.com/feichai0017/NoKV/engine/lsm/flush"
	"github.com/feichai0017/NoKV/metrics"
)

// flushPool runs the flush worker pool: it owns the per-shard FIFO queue
// (flush.Runtime) and the worker WaitGroup. Each task drains an immutable
// memtable through the install callback, removes it from the shard's
// immutables list, and releases the memtable.
type flushPool struct {
	queue   *flush.Runtime[*memTable]
	wg      sync.WaitGroup
	install func(*memTable) error
}

func newFlushPool(shardCount int, install func(*memTable) error) *flushPool {
	return &flushPool{
		queue:   flush.New[*memTable](shardCount),
		install: install,
	}
}

// Submit enqueues mt for flush. The pool takes a reference on mt and
// releases it after install completes.
func (p *flushPool) Submit(mt *memTable) error {
	if mt == nil {
		return nil
	}
	if mt.shard == nil {
		return ErrFlushNilMemtable
	}
	mt.IncrRef()
	if err := p.queue.Enqueue(mt.shard.id, mt); err != nil {
		mt.DecrRef()
		return err
	}
	return nil
}

// Start spins up n worker goroutines. Each pulls one task at a time from
// queue.Next, runs install, removes the memtable from its shard's
// immutables list, and closes it.
func (p *flushPool) Start(n int) {
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		p.wg.Go(func() {
			for {
				task, ok := p.queue.Next()
				if !ok {
					return
				}
				mt := task.Payload
				if mt == nil {
					p.queue.MarkDone(task)
					continue
				}
				p.process(task, mt)
			}
		})
	}
}

func (p *flushPool) process(task *flush.Task[*memTable], mt *memTable) {
	defer mt.DecrRef()
	if err := p.install(mt); err != nil {
		p.queue.MarkDone(task)
		return
	}
	p.queue.MarkInstalled(task)
	if s := mt.shard; s != nil {
		s.lock.Lock()
		for idx, imm := range s.immutables {
			if imm == mt {
				s.immutables = append(s.immutables[:idx], s.immutables[idx+1:]...)
				break
			}
		}
		s.lock.Unlock()
	}
	_ = mt.close()
	p.queue.MarkDone(task)
}

// Pending returns the number of tasks still queued or in flight.
func (p *flushPool) Pending() int64 {
	if p == nil || p.queue == nil {
		return 0
	}
	return p.queue.Stats().Pending
}

// Stats returns the underlying queue metrics snapshot.
func (p *flushPool) Stats() metrics.FlushMetrics {
	if p == nil || p.queue == nil {
		return metrics.FlushMetrics{}
	}
	return p.queue.Stats()
}

// Close stops accepting new submissions, waits for workers to drain, and
// returns the queue's close error.
func (p *flushPool) Close() error {
	if p == nil {
		return nil
	}
	err := p.queue.Close()
	p.wg.Wait()
	return err
}
