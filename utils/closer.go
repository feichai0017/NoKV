package utils

import "sync"

// Closer _用于资源回收的信号控制
type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{}
}

// NewCloser _
func NewCloser() *Closer {
	closer := &Closer{waiting: sync.WaitGroup{}}
	closer.CloseSignal = make(chan struct{})
	return closer
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