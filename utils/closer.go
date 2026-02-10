package utils

import (
	"context"
	"sync"
)

var (
	dummyCloserChan <-chan struct{}
)

// Closer coordinates shutdown and resource cleanup.
type Closer struct {
	waiting sync.WaitGroup

	ctx         context.Context
	CloseSignal chan struct{}
	cancel      context.CancelFunc
}

// NewCloser returns a Closer with an open CloseSignal channel.
func NewCloser() *Closer {
	closer := &Closer{waiting: sync.WaitGroup{}}
	closer.CloseSignal = make(chan struct{})
	return closer
}

// NewCloserInitial creates a new value for the API.
func NewCloserInitial(initial int) *Closer {
	ret := &Closer{}
	ret.ctx, ret.cancel = context.WithCancel(context.Background())
	ret.waiting.Add(initial)
	return ret
}

// Close signals downstream goroutines and waits for them to finish.
func (c *Closer) Close() {
	close(c.CloseSignal)
	c.waiting.Wait()
}

// Done marks a goroutine as finished.
func (c *Closer) Done() {
	c.waiting.Done()
}

// Add adjusts the WaitGroup counter.
func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}

// HasBeenClosed is part of the exported receiver API.
func (c *Closer) HasBeenClosed() <-chan struct{} {
	if c == nil {
		return dummyCloserChan
	}
	return c.ctx.Done()
}

// SignalAndWait is part of the exported receiver API.
func (c *Closer) SignalAndWait() {
	c.Signal()
	c.Wait()
}

// Signal is part of the exported receiver API.
func (c *Closer) Signal() {
	c.cancel()
}

// Wait is part of the exported receiver API.
func (c *Closer) Wait() {
	c.waiting.Wait()
}
