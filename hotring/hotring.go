package hotring

import (
	"sort"
	"sync"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
)

type HashFn func(string) uint32

// Item captures a hot key and its access counter.
type Item struct {
	Key   string
	Count int32
}

// HotRing keeps track of frequently accessed keys using a per-bucket ring.
type HotRing struct {
	mu       sync.RWMutex
	hashFn   HashFn
	hashMask uint32
	tables   []*Node

	windowSlots   int
	windowSlotDur time.Duration

	decayInterval time.Duration
	decayShift    uint32
	decayStop     chan struct{}
	decayWG       sync.WaitGroup
}

const defaultTableBits = 12 // 4096 buckets by default

// NewHotRing builds a ring with 2^bits buckets. When fn is nil a fast xxhash-based hash is used.
func NewHotRing(bits uint8, fn HashFn) *HotRing {
	if bits == 0 || bits > 20 {
		bits = defaultTableBits
	}
	if fn == nil {
		fn = defaultHash
	}
	size := 1 << bits
	mask := uint32(size - 1)
	return &HotRing{
		hashFn:   fn,
		hashMask: mask,
		tables:   make([]*Node, size),
	}
}

// EnableSlidingWindow configures the ring to maintain a time-based sliding window.
// slots specifies how many buckets to retain, while slotDuration controls how long
// each bucket remains active. Passing non-positive values disables the window.
func (h *HotRing) EnableSlidingWindow(slots int, slotDuration time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if slots <= 0 || slotDuration <= 0 {
		h.windowSlots = 0
		h.windowSlotDur = 0
		return
	}
	h.windowSlots = slots
	h.windowSlotDur = slotDuration
}

// EnableDecay applies periodic right-shift decay to the raw counters.
// interval <= 0 or shift == 0 disables background decay.
func (h *HotRing) EnableDecay(interval time.Duration, shift uint32) {
	h.stopDecay()
	if interval <= 0 || shift == 0 {
		return
	}
	h.mu.Lock()
	if h.decayStop != nil {
		close(h.decayStop)
		h.decayStop = nil
	}
	h.decayInterval = interval
	h.decayShift = shift
	stop := make(chan struct{})
	h.decayStop = stop
	h.decayWG.Add(1)
	h.mu.Unlock()

	go h.decayLoop(stop, interval, shift)
}

// Close releases background resources attached to the ring.
func (h *HotRing) Close() {
	h.stopDecay()
}

func defaultHash(key string) uint32 {
	return uint32(xxhash.Sum64String(key))
}

func (h *HotRing) stopDecay() {
	h.mu.Lock()
	stop := h.decayStop
	if stop != nil {
		close(stop)
		h.decayStop = nil
	}
	h.mu.Unlock()
	if stop != nil {
		h.decayWG.Wait()
	}
}

func (h *HotRing) decayLoop(stop <-chan struct{}, interval time.Duration, shift uint32) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		h.decayWG.Done()
	}()
	for {
		select {
		case <-ticker.C:
			h.applyDecay(shift)
		case <-stop:
			return
		}
	}
}

func (h *HotRing) applyDecay(shift uint32) {
	if shift == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, head := range h.tables {
		if head == nil {
			continue
		}
		node := head
		for {
			node.decay(shift)
			next := node.Next()
			if next == nil || next == head {
				break
			}
			node = next
		}
	}
}

func (h *HotRing) currentSlot() int64 {
	if h == nil || h.windowSlots <= 0 || h.windowSlotDur <= 0 {
		return 0
	}
	return time.Now().UnixNano() / h.windowSlotDur.Nanoseconds()
}

func (h *HotRing) nodeCountLocked(node *Node, slot int64) int32 {
	if node == nil {
		return 0
	}
	if h.windowSlots > 0 {
		return node.windowTotalAt(h.windowSlots, slot)
	}
	return node.GetCounter()
}

func (h *HotRing) incrementNodeLocked(node *Node, slot int64) int32 {
	if node == nil {
		return 0
	}
	node.Increment()
	if h.windowSlots > 0 {
		return node.incrementWindow(h.windowSlots, slot)
	}
	return node.GetCounter()
}

// Touch records a key access and returns the updated counter.
func (h *HotRing) Touch(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	slot := h.currentSlot()
	h.mu.Lock()
	defer h.mu.Unlock()

	node := h.searchLocked(key)
	if node == nil {
		var inserted bool
		node, inserted = h.insertLocked(key)
		if !inserted {
			if node == nil {
				return 0
			}
		} else {
			node.ResetCounter()
			if h.windowSlots > 0 {
				node.ensureWindow(h.windowSlots, slot)
			}
		}
	}
	return h.incrementNodeLocked(node, slot)
}

// Frequency returns the current access counter for key without mutating state.
func (h *HotRing) Frequency(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	slot := h.currentSlot()
	if h.windowSlots > 0 {
		h.mu.Lock()
		defer h.mu.Unlock()
	} else {
		h.mu.RLock()
		defer h.mu.RUnlock()
	}
	node := h.searchLocked(key)
	return h.nodeCountLocked(node, slot)
}

// TouchAndClamp increments the counter if below the provided limit and reports
// whether the key should be considered throttled.
func (h *HotRing) TouchAndClamp(key string, limit int32) (count int32, limited bool) {
	if h == nil || key == "" {
		return 0, false
	}
	if limit <= 0 {
		return h.Touch(key), false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	slot := h.currentSlot()
	node := h.searchLocked(key)
	if node == nil {
		node, inserted := h.insertLocked(key)
		if !inserted || node == nil {
			return 0, false
		}
		node.ResetCounter()
		if h.windowSlots > 0 {
			node.ensureWindow(h.windowSlots, slot)
		}
		count = h.incrementNodeLocked(node, slot)
		return count, count >= limit
	}
	current := h.nodeCountLocked(node, slot)
	if current >= limit {
		return current, true
	}
	count = h.incrementNodeLocked(node, slot)
	return count, count >= limit
}

func (h *HotRing) Remove(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	toDel := h.searchLocked(key)
	if toDel == nil {
		return
	}
	hashVal := h.hashFn(key)
	index := hashVal & h.hashMask

	prev := toDel

	// 遍历找到待删除节点的前一个节点
	for {
		if next := prev.Next(); next != nil && next != toDel {
			prev = next
			continue
		}
		break
	}

	prev.SetNext(toDel.Next())

	if h.tables[index] == toDel {
		if prev == toDel {
			h.tables[index] = nil
		} else {
			h.tables[index] = toDel.Next()
		}
	}
}

// TopN returns at most n hot keys ordered by access count (descending).
func (h *HotRing) TopN(n int) []Item {
	if h == nil || n <= 0 {
		return nil
	}
	slot := h.currentSlot()
	if h.windowSlots > 0 {
		h.mu.Lock()
		defer h.mu.Unlock()
	} else {
		h.mu.RLock()
		defer h.mu.RUnlock()
	}

	var items []Item
	for _, head := range h.tables {
		if head == nil {
			continue
		}
		node := head
		for {
			items = append(items, Item{
				Key:   node.key,
				Count: h.nodeCountLocked(node, slot),
			})
			next := node.Next()
			if next == nil || next == head {
				break
			}
			node = next
		}
	}
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > n {
		items = append([]Item(nil), items[:n]...)
	} else {
		items = append([]Item(nil), items...)
	}
	return items
}

// KeysAbove returns all keys whose counters are at least threshold.
func (h *HotRing) KeysAbove(threshold int32) []Item {
	if h == nil || threshold <= 0 {
		return nil
	}
	slot := h.currentSlot()
	if h.windowSlots > 0 {
		h.mu.Lock()
		defer h.mu.Unlock()
	} else {
		h.mu.RLock()
		defer h.mu.RUnlock()
	}
	var items []Item
	for _, head := range h.tables {
		if head == nil {
			continue
		}
		node := head
		for {
			if cnt := h.nodeCountLocked(node, slot); cnt >= threshold {
				items = append(items, Item{Key: node.key, Count: cnt})
			}
			next := node.Next()
			if next == nil || next == head {
				break
			}
			node = next
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	return items
}

// searchLocked finds the node for key. Caller must hold at least a read lock.
func (h *HotRing) searchLocked(key string) *Node {
	hashVal := h.hashFn(key)
	index, tag := hashVal&h.hashMask, hashVal&(^h.hashMask)

	compareItem := NewCompareItem(key, tag)

	var prev, next *Node
	var res *Node

	head := h.tables[index]
	switch {
	case head == nil:
		return nil
	case head.Next() == head:
		if compareItem.Equal(head) {
			res = head
		}
	default:
		prev = head
		next = prev.Next()
		for {
			if compareItem.Equal(prev) {
				res = prev
				break
			}

			if (prev.Less(compareItem) && compareItem.Less(next)) ||
				(compareItem.Less(prev) && next.Less(prev)) ||
				(next.Less(prev) && prev.Less(compareItem)) {
				break
			}
			prev = next
			next = next.Next()
			if next == nil {
				break
			}
		}
	}
	return res
}

func (h *HotRing) insertLocked(key string) (*Node, bool) {
	hashVal := h.hashFn(key)
	index, tag := hashVal&h.hashMask, hashVal&(^h.hashMask)

	newItem := NewNode(key, tag)

	switch head := h.tables[index]; {
	case head == nil:
		h.tables[index] = newItem
		newItem.SetNext(newItem)
	case head.Next() == head:
		newItem.SetNext(head)
		head.SetNext(newItem)
		h.tables[index] = newItem
	default:
		prev := head
		next := prev.Next()
		for {
			if newItem.Equal(prev) {
				return prev, false
			}

			if (prev.Less(newItem) && newItem.Less(next)) ||
				(newItem.Less(next) && next.Less(prev)) ||
				(next.Less(prev) && prev.Less(newItem)) {
				newItem.SetNext(next)
				prev.SetNext(newItem)
				break
			}

			prev = next
			next = next.Next()
			if next == nil {
				break
			}
			if next == head {
				// Insert at tail.
				newItem.SetNext(next)
				prev.SetNext(newItem)
				break
			}
		}
	}
	return newItem, true
}
