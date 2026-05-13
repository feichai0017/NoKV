package peras

import (
	"context"
	"sync"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type perasSealLane struct {
	owner  *Runtime
	jobs   chan perasSealRequest
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

type perasSealRequest struct {
	ctx    context.Context
	holder *fsperas.Holder
	job    perasFlushJob
	done   chan error
}

func newPerasSealLane(owner *Runtime, workers int) *perasSealLane {
	if workers <= 0 {
		workers = 1
	}
	lane := &perasSealLane{
		owner:  owner,
		jobs:   make(chan perasSealRequest, workers*4),
		closed: make(chan struct{}),
	}
	lane.wg.Add(workers)
	for range workers {
		go lane.worker()
	}
	return lane
}

func (l *perasSealLane) publish(ctx context.Context, holder *fsperas.Holder, job perasFlushJob) error {
	if l == nil || l.owner == nil {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.closed:
		return ErrRuntimeClosed
	default:
	}
	done := make(chan error, 1)
	req := perasSealRequest{ctx: ctx, holder: holder, job: job, done: done}
	select {
	case l.jobs <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-l.closed:
		return ErrRuntimeClosed
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-l.closed:
		return ErrRuntimeClosed
	}
}

func (l *perasSealLane) close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		close(l.closed)
	})
	l.wg.Wait()
}

func (l *perasSealLane) depth() int {
	if l == nil {
		return 0
	}
	return len(l.jobs)
}

func (l *perasSealLane) capacity() int {
	if l == nil {
		return 0
	}
	return cap(l.jobs)
}

func (l *perasSealLane) worker() {
	defer l.wg.Done()
	for {
		select {
		case req := <-l.jobs:
			l.run(req)
		case <-l.closed:
			return
		}
	}
}

func (l *perasSealLane) run(req perasSealRequest) {
	if err := req.ctx.Err(); err != nil {
		req.done <- err
		return
	}
	req.done <- l.owner.publishSegmentSeal(req.ctx, req.holder, req.job)
}
