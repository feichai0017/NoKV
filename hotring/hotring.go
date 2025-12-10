package hotring

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
)

type HashFn func(string) uint32

// Item captures a hot key and its access counter.
type Item struct {
	Key   string
	Count int32
}

// HotRing keeps track of frequently accessed keys using lock-free bucketed lists.
type HotRing struct {
	hashFn   HashFn
	hashMask uint32
	buckets  []atomic.Pointer[Node]

	windowSlots   atomic.Int32
	windowSlotDur atomic.Int64

	decayMu   sync.Mutex
	decayStop chan struct{}
	decayWG   sync.WaitGroup
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
		buckets:  make([]atomic.Pointer[Node], size),
	}
}

// EnableSlidingWindow configures the ring to maintain a time-based sliding window.
// slots specifies how many buckets to retain, while slotDuration controls how long
// each bucket remains active. Passing non-positive values disables the window.
func (h *HotRing) EnableSlidingWindow(slots int, slotDuration time.Duration) {
	if slots <= 0 || slotDuration <= 0 {
		h.windowSlots.Store(0)
		h.windowSlotDur.Store(0)
		return
	}
	h.windowSlots.Store(int32(slots))
	h.windowSlotDur.Store(slotDuration.Nanoseconds())
}

// EnableDecay applies periodic right-shift decay to the raw counters.
// interval <= 0 or shift == 0 disables background decay.
func (h *HotRing) EnableDecay(interval time.Duration, shift uint32) {
	h.stopDecay()
	if interval <= 0 || shift == 0 {
		return
	}
	h.decayMu.Lock()
	stop := make(chan struct{})
	h.decayStop = stop
	h.decayWG.Add(1)
	h.decayMu.Unlock()

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
	h.decayMu.Lock()
	stop := h.decayStop
	if stop != nil {
		close(stop)
		h.decayStop = nil
	}
	h.decayMu.Unlock()
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
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			node.decay(shift)
		}
	}
}

func (h *HotRing) slotState() (slot int64, slots int) {
	slots = int(h.windowSlots.Load())
	slotDur := h.windowSlotDur.Load()
	if slots <= 0 || slotDur <= 0 {
		return 0, 0
	}
	return time.Now().UnixNano() / slotDur, slots
}

func (h *HotRing) nodeCount(node *Node, slots int, slot int64) int32 {
	if node == nil {
		return 0
	}
	if slots > 0 {
		return node.windowTotalAt(slots, slot)
	}
	return node.GetCounter()
}

func (h *HotRing) incrementNode(node *Node, slots int, slot int64) int32 {
	if node == nil {
		return 0
	}
	node.Increment()
	if slots > 0 {
		return node.incrementWindow(slots, slot)
	}
	return node.GetCounter()
}

// Touch records a key access and returns the updated counter.
func (h *HotRing) Touch(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	slot, slots := h.slotState()
	index, tag := h.hashParts(key)
	compareItem := NewCompareItem(key, tag)
	node, inserted := h.findOrInsert(index, compareItem, slots, slot)
	if node == nil {
		return 0
	}
	if inserted && slots == 0 {
		node.ResetCounter()
	}
	return h.incrementNode(node, slots, slot)
}

// Frequency returns the current access counter for key without mutating state.
func (h *HotRing) Frequency(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	slot, slots := h.slotState()
	index, tag := h.hashParts(key)
	node := h.search(index, NewCompareItem(key, tag))
	return h.nodeCount(node, slots, slot)
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
	slot, slots := h.slotState()
	index, tag := h.hashParts(key)
	compareItem := NewCompareItem(key, tag)
	node, inserted := h.findOrInsert(index, compareItem, slots, slot)
	if node == nil {
		return 0, false
	}
	if inserted && slots == 0 {
		node.ResetCounter()
	}
	current := h.nodeCount(node, slots, slot)
	if current >= limit {
		return current, true
	}
	count = h.incrementNode(node, slots, slot)
	return count, count >= limit
}

func (h *HotRing) Remove(key string) {
	if h == nil || key == "" {
		return
	}
	index, tag := h.hashParts(key)
	compareItem := NewCompareItem(key, tag)
	bucket := &h.buckets[index]
	for {
		head := bucket.Load()
		var prev *Node
		curr := head
		for curr != nil && !compareItem.Less(curr) {
			if compareItem.Equal(curr) {
				next := curr.Next()
				if prev == nil {
					if bucket.CompareAndSwap(head, next) {
						return
					}
					break
				}
				if prev.CompareAndSwapNext(curr, next) {
					return
				}
				break
			}
			prev = curr
			curr = curr.Next()
		}
		if curr == nil || compareItem.Less(curr) {
			return
		}
	}
}

// TopN returns at most n hot keys ordered by access count (descending).
func (h *HotRing) TopN(n int) []Item {
	if h == nil || n <= 0 {
		return nil
	}
	slot, slots := h.slotState()

	var items []Item
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			items = append(items, Item{
				Key:   node.key,
				Count: h.nodeCount(node, slots, slot),
			})
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
	slot, slots := h.slotState()
	var items []Item
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			if cnt := h.nodeCount(node, slots, slot); cnt >= threshold {
				items = append(items, Item{Key: node.key, Count: cnt})
			}
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

func (h *HotRing) hashParts(key string) (index uint32, tag uint32) {
	hashVal := h.hashFn(key)
	return hashVal & h.hashMask, hashVal & (^h.hashMask)
}

func (h *HotRing) search(index uint32, compareItem *Node) *Node {
	for node := h.buckets[index].Load(); node != nil; node = node.Next() {
		if compareItem.Equal(node) {
			return node
		}
		if compareItem.Less(node) {
			return nil
		}
	}
	return nil
}

// findOrInsert keeps the bucket sorted by (tag,key) using CAS on the head or predecessor.
func (h *HotRing) findOrInsert(index uint32, compareItem *Node, slots int, slot int64) (*Node, bool) {
	bucket := &h.buckets[index]
	for {
		head := bucket.Load()
		var prev *Node
		curr := head
		for curr != nil {
			if compareItem.Equal(curr) {
				return curr, false
			}
			if compareItem.Less(curr) {
				break
			}
			prev = curr
			curr = curr.Next()
		}

		newNode := NewNode(compareItem.key, compareItem.tag)
		if slots > 0 {
			newNode.ResetCounterWithWindow(slots, slot)
		}
		newNode.StoreNext(curr)

		if prev == nil {
			if bucket.CompareAndSwap(head, newNode) {
				return newNode, true
			}
		} else if prev.CompareAndSwapNext(curr, newNode) {
			return newNode, true
		}
	}
}
