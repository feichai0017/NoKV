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

func (c *Closer) HasBeenClosed() <-chan struct{} {
	if c == nil {
		return dummyCloserChan
	}
	return c.ctx.Done()
}

func (c *Closer) SignalAndWait() {
	c.Signal()
	c.Wait()
}

func (c *Closer) Signal() {
	c.cancel()
}

func (c *Closer) Wait() {
	c.waiting.Wait()
}
