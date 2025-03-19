package hotring

import (
	"sync/atomic"
	"unsafe"
)

// HeadPointer 头指针的组成
type HeadPointer struct {
	ringCounter int32
	headPtr     unsafe.Pointer
}

func (hp *HeadPointer) GetNode() *Node {
	return (*Node)(atomic.LoadPointer(&hp.headPtr))
}

func (hp *HeadPointer) Increment() {
	atomic.AddInt32(&hp.ringCounter, 1)
}

func (hp *HeadPointer) Counter() int32 {
	return hp.ringCounter
}

func (hp *HeadPointer) SetNode(node *Node) {
	for {
		if atomic.CompareAndSwapPointer(&hp.headPtr, hp.headPtr, unsafe.Pointer(node)) {
			return
		}
	}
}