package hotring

import (
	"sync/atomic"
	"unsafe"
)

type Node struct {
	key   string
	tag   uint32
	next  unsafe.Pointer
	count int32

	window       []int32
	windowTotal  int32
	windowPos    int
	windowSlotID int64
}

func NewNode(key string, tag uint32) *Node {
	return &Node{
		key: key,
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

func (n *Node) GetCounter() int32 {
	return atomic.LoadInt32(&n.count)
}

func (n *Node) ResetCounter() {
	atomic.StoreInt32(&n.count, 0)
	n.windowTotal = 0
	if n.window != nil {
		for i := range n.window {
			n.window[i] = 0
		}
		n.windowPos = 0
		n.windowSlotID = 0
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

func (n *Node) ensureWindow(slots int, slotID int64) {
	if slots <= 0 {
		return
	}
	if len(n.window) != slots {
		n.window = make([]int32, slots)
		n.windowPos = int(slotID % int64(slots))
		n.windowSlotID = slotID
		n.windowTotal = 0
		return
	}
	if n.windowSlotID == 0 {
		n.windowSlotID = slotID
	}
}

func (n *Node) advanceWindow(slots int, slotID int64) {
	if slots <= 0 {
		return
	}
	n.ensureWindow(slots, slotID)
	if slotID <= n.windowSlotID {
		return
	}
	steps := slotID - n.windowSlotID
	if steps >= int64(slots) {
		for i := range n.window {
			n.window[i] = 0
		}
		n.windowTotal = 0
		n.windowPos = int(slotID % int64(slots))
		n.windowSlotID = slotID
		return
	}
	for range steps {
		n.windowPos = (n.windowPos + 1) % slots
		n.windowTotal -= n.window[n.windowPos]
		n.window[n.windowPos] = 0
	}
	n.windowSlotID = slotID
}

func (n *Node) incrementWindow(slots int, slotID int64) int32 {
	if slots <= 0 {
		return n.GetCounter()
	}
	n.advanceWindow(slots, slotID)
	n.window[n.windowPos]++
	n.windowTotal++
	return n.windowTotal
}

func (n *Node) windowTotalAt(slots int, slotID int64) int32 {
	if slots <= 0 {
		return n.GetCounter()
	}
	n.advanceWindow(slots, slotID)
	return n.windowTotal
}

func (n *Node) decay(shift uint32) {
	if shift == 0 {
		return
	}
	if n.count == 0 {
		return
	}
	n.count = int32(int64(n.count) >> shift)
}
