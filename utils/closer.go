package utils

import (
	"context"
	"sync"
)

var (
	dummyCloserChan <-chan struct{}
)

// Closer _用于资源回收的信号控制
type Closer struct {
	waiting sync.WaitGroup

	ctx         context.Context
	CloseSignal chan struct{}
	cancel      context.CancelFunc
}

// NewCloser _
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

// Close 上游通知下游协程进行资源回收，并等待协程通知回收完毕
func (c *Closer) Close() {
	close(c.CloseSignal)
	c.waiting.Wait()
}

// Done 标示协程已经完成资源回收，通知上游正式关闭
func (c *Closer) Done() {
	c.waiting.Done()
}

// Add 添加wait 计数
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