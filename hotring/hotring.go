package hotring

import (
	"sort"
	"sync"

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
	addrMask uint32
	R        uint8
	hashFn   HashFn
	hashMask uint32

	findCnt    uint32
	maxFindCnt uint32
	minFindCnt uint32

	tables []*Node
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
		R:        bits,
		hashFn:   fn,
		hashMask: mask,
		addrMask: ^mask,
		tables:   make([]*Node, size),
	}
}

func defaultHash(key string) uint32 {
	return uint32(xxhash.Sum64String(key))
}

// Touch records a key access and returns the updated counter.
func (h *HotRing) Touch(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if node := h.searchLocked(key, true); node != nil {
		return node.GetCounter()
	}
	node, inserted := h.insertLocked(key, key)
	if !inserted {
		// Shouldn't happen because search returned nil, but guard anyway.
		if node != nil {
			return node.GetCounter()
		}
		return 0
	}
	// First access for this key.
	node.ResetCounter()
	return node.Increment()
}

// Frequency returns the current access counter for key without mutating state.
func (h *HotRing) Frequency(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	node := h.searchLocked(key, false)
	if node == nil {
		return 0
	}
	return node.GetCounter()
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
	node := h.searchLocked(key, false)
	if node == nil {
		node, inserted := h.insertLocked(key, key)
		if !inserted || node == nil {
			return 0, false
		}
		node.ResetCounter()
		count = node.Increment()
		return count, count >= limit
	}
	current := node.GetCounter()
	if current >= limit {
		return current, true
	}
	count = node.Increment()
	return count, count >= limit
}


// Search returns the node for key, incrementing its counter.
func (h *HotRing) Search(key string) *Node {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.searchLocked(key, true)
}

// Insert adds a keyed node when absent.
func (h *HotRing) Insert(key, val string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, inserted := h.insertLocked(key, val)
	return inserted
}

func (h *HotRing) Remove(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	toDel := h.searchLocked(key, false)
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

func (h *HotRing) Update(key, val string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	res := h.searchLocked(key, false)
	if res == nil {
		return false
	}

	res.val = val
	hashVal := h.hashFn(key)
	index := hashVal & h.hashMask
	h.tables[index] = res
	return true
}

// TopN returns at most n hot keys ordered by access count (descending).
func (h *HotRing) TopN(n int) []Item {
	if h == nil || n <= 0 {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	var items []Item
	for _, head := range h.tables {
		if head == nil {
			continue
		}
		node := head
		for {
			items = append(items, Item{
				Key:   node.key,
				Count: node.GetCounter(),
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
	h.mu.RLock()
	defer h.mu.RUnlock()
	var items []Item
	for _, head := range h.tables {
		if head == nil {
			continue
		}
		node := head
		for {
			if cnt := node.GetCounter(); cnt >= threshold {
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

// searchLocked finds the node for key; if increment is true the access counter is incremented.
func (h *HotRing) searchLocked(key string, increment bool) *Node {
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

	if res != nil && increment {
		res.Increment()
	}
	return res
}

func (h *HotRing) insertLocked(key, val string) (*Node, bool) {
	hashVal := h.hashFn(key)
	index, tag := hashVal&h.hashMask, hashVal&(^h.hashMask)

	newItem := NewNode(key, val, tag)

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
