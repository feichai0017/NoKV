package utils

import "sync"

// Throttle is a small wrapper around ants pool that limits concurrent tasks
// and collects their errors.
type Throttle struct {
	once      sync.Once
	wg        sync.WaitGroup
	errCh     chan error
	finishErr error
	pool      *Pool
}

// NewThrottle creates a new throttle with a max number of workers.
func NewThrottle(max int) *Throttle {
	if max <= 0 {
		max = 1
	}
	return &Throttle{
		errCh: make(chan error, max),
		pool:  NewPool(max, "Throttle"),
	}
}

// Go submits a task to the underlying goroutine pool.
func (t *Throttle) Go(fn func() error) error {
	if fn == nil {
		return nil
	}
	t.wg.Add(1)
	return t.pool.Submit(func() {
		defer t.wg.Done()
		if err := fn(); err != nil {
			t.errCh <- err
		}
	})
}

// Finish waits until all workers have finished working. It returns the first
// error encountered.
func (t *Throttle) Finish() error {
	t.once.Do(func() {
		t.wg.Wait()
		close(t.errCh)
		for err := range t.errCh {
			if err != nil {
				t.finishErr = err
				break
			}
		}
		if t.pool != nil {
			t.pool.Release()
		}
	})
	return t.finishErr
}
