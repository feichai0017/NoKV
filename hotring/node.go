package hotring

import (
	"runtime"
	"sync/atomic"
)

type Node struct {
	key   string
	tag   uint32
	next  atomic.Pointer[Node]
	count int32

	window       []int32
	windowTotal  int32
	windowPos    int
	windowSlotID int64
	windowLock   uint32
}

func NewNode(key string, tag uint32) *Node {
	return &Node{
		key: key,
		tag: tag,
	}
}

func (n *Node) Next() *Node {
	return n.next.Load()
}

// Less compares by tag first, then by key when tags are equal.
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
	n.ResetCounterWithWindow(0, 0)
}

func (n *Node) SetNext(next *Node) {
	n.next.Store(next)
}

func (n *Node) CompareAndSwapNext(old, next *Node) bool {
	return n.next.CompareAndSwap(old, next)
}

func (n *Node) Increment() int32 {
	return atomic.AddInt32(&n.count, 1)
}

func (n *Node) lockWindow() {
	for !atomic.CompareAndSwapUint32(&n.windowLock, 0, 1) {
		runtime.Gosched()
	}
}

func (n *Node) unlockWindow() {
	atomic.StoreUint32(&n.windowLock, 0)
}

func (n *Node) resetWindowLocked(slots int, slotID int64) {
	if slots <= 0 {
		n.window = nil
		n.windowTotal = 0
		n.windowPos = 0
		n.windowSlotID = 0
		return
	}
	if len(n.window) != slots {
		n.window = make([]int32, slots)
	} else {
		for i := range n.window {
			n.window[i] = 0
		}
	}
	n.windowTotal = 0
	n.windowPos = int(slotID % int64(slots))
	n.windowSlotID = slotID
}

func (n *Node) ensureWindowLocked(slots int, slotID int64) {
	if slots <= 0 {
		return
	}
	if len(n.window) != slots || n.windowSlotID == 0 {
		n.resetWindowLocked(slots, slotID)
	}
}

func (n *Node) advanceWindowLocked(slots int, slotID int64) {
	if slots <= 0 {
		return
	}
	n.ensureWindowLocked(slots, slotID)
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
	for steps > 0 {
		n.windowPos = (n.windowPos + 1) % slots
		n.windowTotal -= n.window[n.windowPos]
		n.window[n.windowPos] = 0
		steps--
	}
	n.windowSlotID = slotID
}

func (n *Node) incrementWindow(slots int, slotID int64) int32 {
	if slots <= 0 {
		return n.GetCounter()
	}
	n.lockWindow()
	n.advanceWindowLocked(slots, slotID)
	n.window[n.windowPos]++
	n.windowTotal++
	total := n.windowTotal
	n.unlockWindow()
	return total
}

func (n *Node) windowTotalAt(slots int, slotID int64) int32 {
	if slots <= 0 {
		return n.GetCounter()
	}
	n.lockWindow()
	n.advanceWindowLocked(slots, slotID)
	total := n.windowTotal
	n.unlockWindow()
	return total
}

func (n *Node) ResetCounterWithWindow(slots int, slotID int64) {
	atomic.StoreInt32(&n.count, 0)
	n.lockWindow()
	n.resetWindowLocked(slots, slotID)
	n.unlockWindow()
}

func (n *Node) decay(shift uint32) {
	if shift == 0 {
		return
	}
	for {
		cur := atomic.LoadInt32(&n.count)
		if cur == 0 {
			return
		}
		decayed := int32(int64(cur) >> shift)
		if atomic.CompareAndSwapInt32(&n.count, cur, decayed) {
			return
		}
	}
}
