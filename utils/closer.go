package utils

import (
	"sync"
)

var (
	dummyCloserChan <-chan struct{}
)

// Closer coordinates shutdown and resource cleanup.
type Closer struct {
	waiting   sync.WaitGroup
	closed    chan struct{}
	closeOnce sync.Once
}

// NewCloser returns a Closer with an open shutdown channel.
func NewCloser() *Closer {
	return &Closer{
		waiting: sync.WaitGroup{},
		closed:  make(chan struct{}),
	}
}

// NewCloserInitial creates a closer with an initial WaitGroup count.
func NewCloserInitial(initial int) *Closer {
	ret := NewCloser()
	if initial > 0 {
		ret.waiting.Add(initial)
	}
	return ret
}

// Close signals downstream goroutines and waits for them to finish.
func (c *Closer) Close() {
	if c == nil {
		return
	}
	c.signal()
	c.waiting.Wait()
}

// Done marks a goroutine as finished.
func (c *Closer) Done() {
	if c == nil {
		return
	}
	c.waiting.Done()
}

// Add adjusts the WaitGroup counter.
func (c *Closer) Add(n int) {
	if c == nil {
		return
	}
	c.waiting.Add(n)
}

// Closed returns a channel that is closed exactly once when shutdown begins.
func (c *Closer) Closed() <-chan struct{} {
	if c == nil || c.closed == nil {
		return dummyCloserChan
	}
	return c.closed
}

// signal notifies listeners only once and is safe under concurrent Close calls.
func (c *Closer) signal() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		if c.closed != nil {
			close(c.closed)
		}
	})
}
