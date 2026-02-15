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
	closeOnce   sync.Once
}

// NewCloser returns a Closer with an open CloseSignal channel.
func NewCloser() *Closer {
	closer := &Closer{waiting: sync.WaitGroup{}}
	closer.ctx, closer.cancel = context.WithCancel(context.Background())
	closer.CloseSignal = make(chan struct{})
	return closer
}

// NewCloserInitial creates a closer with an initial WaitGroup count and a
// dedicated close signal/context pair for cooperative shutdown.
func NewCloserInitial(initial int) *Closer {
	ret := &Closer{}
	ret.ctx, ret.cancel = context.WithCancel(context.Background())
	ret.CloseSignal = make(chan struct{})
	ret.waiting.Add(initial)
	return ret
}

// Close signals downstream goroutines and waits for them to finish.
func (c *Closer) Close() {
	c.signal()
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

func (c *Closer) HasBeenClosed() <-chan struct{} {
	if c == nil || c.ctx == nil {
		return dummyCloserChan
	}
	return c.ctx.Done()
}

func (c *Closer) SignalAndWait() {
	c.Signal()
	c.Wait()
}

func (c *Closer) Signal() {
	c.signal()
}

func (c *Closer) Wait() {
	c.waiting.Wait()
}

// signal notifies listeners only once and is safe under concurrent Signal/Close calls.
func (c *Closer) signal() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.CloseSignal != nil {
			close(c.CloseSignal)
		}
	})
}
