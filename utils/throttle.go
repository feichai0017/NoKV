// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import "sync"

// Throttle is a small wrapper around ants pool that limits concurrent tasks
// and collects their errors.
type Throttle struct {
	once      sync.Once
	wg        sync.WaitGroup
	errMu     sync.Mutex
	finishErr error
	pool      *Pool
}

// NewThrottle creates a new throttle with a max number of workers.
func NewThrottle(max int) *Throttle {
	if max <= 0 {
		max = 1
	}
	return &Throttle{
		pool: NewPool(max, "Throttle"),
	}
}

// Go submits a task to the underlying goroutine pool.
func (t *Throttle) Go(fn func() error) error {
	if fn == nil {
		return nil
	}
	t.wg.Add(1)
	if err := t.pool.Submit(func() {
		defer t.wg.Done()
		if err := fn(); err != nil {
			t.recordError(err)
		}
	}); err != nil {
		t.wg.Done()
		t.recordError(err)
		return err
	}
	return nil
}

// Finish waits until all workers have finished working. It returns the first
// error encountered.
func (t *Throttle) Finish() error {
	t.once.Do(func() {
		t.wg.Wait()
		if t.pool != nil {
			t.pool.Release()
		}
	})
	return t.firstError()
}

func (t *Throttle) recordError(err error) {
	if err == nil {
		return
	}
	t.errMu.Lock()
	defer t.errMu.Unlock()
	if t.finishErr == nil {
		t.finishErr = err
	}
}

func (t *Throttle) firstError() error {
	t.errMu.Lock()
	defer t.errMu.Unlock()
	return t.finishErr
}
