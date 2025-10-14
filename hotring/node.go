package hotring

import (
	"sync/atomic"
	"unsafe"
)

type Node struct {
	key     string
	val     string
	tag     uint32
	incBase uint32
	next    unsafe.Pointer
	hp      unsafe.Pointer
	count   int32
}

func NewNode(key, val string, tag uint32) *Node {
	return &Node{
		key: key,
		val: val,
		tag: tag,
	}
}

// NewCompareItem 我们只需要 Tag 和 Key 来进行比较
func NewCompareItem(key string, tag uint32) *Node {
	return &Node{
		key: key,
		tag: tag,
	}
}

func (n *Node) Next() *Node {
	next := atomic.LoadPointer(&n.next)
	if next != nil {
		return (*Node)(next)
	}
	return nil
}

// Less 先比较节点的 Tag 值，Tag 值相同时，再比较 Key 值大小
func (n *Node) Less(c *Node) bool {
	if c == nil {
		return false
	}

	if n.tag == c.tag {
		return n.key < c.key
	}

	return n.tag < c.tag
}

func (n *Node) Equal(c *Node) bool {
	if c == nil {
		return false
	}

	if n.tag == c.tag && n.key == c.key {
		return true
	}
	return false
}

func (n *Node) GetHead() *HeadPointer {
	return (*HeadPointer)(atomic.LoadPointer(&n.hp))
}

func (n *Node) GetCounter() int32 {
	return atomic.LoadInt32(&n.count)
}

func (n *Node) ResetCounter() {
	atomic.StoreInt32(&n.count, 0)
}

func (n *Node) SetHead(hp *HeadPointer) {
	for {
		cur := atomic.LoadPointer(&n.hp)
		if atomic.CompareAndSwapPointer(&n.hp, cur, unsafe.Pointer(hp)) {
			return
		}
	}
}

func (n *Node) SetNext(next *Node) {
	for {
		cur := atomic.LoadPointer(&n.next)
		if atomic.CompareAndSwapPointer(&n.next, cur, unsafe.Pointer(next)) {
			return
		}
	}
}

func (n *Node) Increment() int32 {
	return atomic.AddInt32(&n.count, 1)
}
