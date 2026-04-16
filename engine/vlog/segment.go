package vlog

import (
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/file"
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
	state    atomic.Uint32
	pinCount atomic.Int32
}

func newSegment(store *file.LogFile, sealed bool) *segment {
	st := segmentActive
	if sealed {
		st = segmentSealed
	}
	seg := &segment{
		store: store,
	}
	seg.state.Store(uint32(st))
	seg.pinCond = sync.NewCond(&seg.pinMu)
	return seg
}

func (s *segment) isSealed() bool {
	return s.state.Load() == uint32(segmentSealed)
}

func (s *segment) isClosing() bool {
	return s.state.Load() == uint32(segmentClosing)
}

func (s *segment) seal() {
	s.state.Store(uint32(segmentSealed))
}

func (s *segment) activate() {
	s.state.Store(uint32(segmentActive))
}

func (s *segment) beginClose() {
	s.state.Store(uint32(segmentClosing))
}

func (s *segment) pinRead() bool {
	if s.isClosing() {
		return false
	}
	s.pinCount.Add(1)
	if s.isClosing() {
		s.pinCount.Add(-1)
		return false
	}
	return true
}

func (s *segment) unpinRead() {
	if s.pinCount.Add(-1) == 0 {
		s.pinMu.Lock()
		s.pinCond.Broadcast()
		s.pinMu.Unlock()
	}
}

// waitForNoPins blocks until all in-flight reads release their pins.
func (s *segment) waitForNoPins() {
	if s.pinCount.Load() == 0 {
		return
	}
	s.pinMu.Lock()
	for s.pinCount.Load() > 0 {
		s.pinCond.Wait()
	}
	s.pinMu.Unlock()
}
