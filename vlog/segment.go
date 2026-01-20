package vlog

import (
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/file"
)

type segmentState uint32

const (
	segmentActive segmentState = iota
	segmentSealed
	segmentClosing
)

type segment struct {
	store    *file.LogFile
	pinMu    sync.Mutex
	pinCond  *sync.Cond
	state    uint32
	pinCount int32
}

func newSegment(store *file.LogFile, sealed bool) *segment {
	st := segmentActive
	if sealed {
		st = segmentSealed
	}
	seg := &segment{
		store: store,
		state: uint32(st),
	}
	seg.pinCond = sync.NewCond(&seg.pinMu)
	return seg
}

func (s *segment) isSealed() bool {
	return atomic.LoadUint32(&s.state) == uint32(segmentSealed)
}

func (s *segment) isClosing() bool {
	return atomic.LoadUint32(&s.state) == uint32(segmentClosing)
}

func (s *segment) seal() {
	atomic.StoreUint32(&s.state, uint32(segmentSealed))
}

func (s *segment) activate() {
	atomic.StoreUint32(&s.state, uint32(segmentActive))
}

func (s *segment) beginClose() {
	atomic.StoreUint32(&s.state, uint32(segmentClosing))
}

func (s *segment) pinRead() bool {
	if s.isClosing() {
		return false
	}
	atomic.AddInt32(&s.pinCount, 1)
	if s.isClosing() {
		atomic.AddInt32(&s.pinCount, -1)
		return false
	}
	return true
}

func (s *segment) unpinRead() {
	if atomic.AddInt32(&s.pinCount, -1) == 0 {
		s.pinMu.Lock()
		s.pinCond.Broadcast()
		s.pinMu.Unlock()
	}
}

// waitForNoPins blocks until all in-flight reads release their pins.
func (s *segment) waitForNoPins() {
	if atomic.LoadInt32(&s.pinCount) == 0 {
		return
	}
	s.pinMu.Lock()
	for atomic.LoadInt32(&s.pinCount) > 0 {
		s.pinCond.Wait()
	}
	s.pinMu.Unlock()
}
