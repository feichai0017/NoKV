package cache

import (
	"container/list"
	"math"
	"sync"
	"unsafe"

	xxhash "github.com/cespare/xxhash/v2"
)

type Cache struct {
	m         sync.RWMutex
	lru       *windowLRU
	slru      *segmentedLRU
	door      *BloomFilter
	c         *cmSketch
	t         int32
	threshold int32
	data      map[uint64]*list.Element
	maxCost   int64
	usedCost  int64
	costFn    func(any) int64
}

// NewCache creates a W-TinyLFU cache whose budget is expressed in entry count.
func NewCache(size int) *Cache {
	return NewWeightedCache(int64(size), size, func(any) int64 { return 1 })
}

// NewWeightedCache creates a W-TinyLFU cache whose budget is enforced by the
// provided cost function. estimatedItems sizes the admission/filter metadata.
func NewWeightedCache(maxCost int64, estimatedItems int, costFn func(any) int64) *Cache {
	if maxCost <= 0 {
		panic("cache: invalid maxCost")
	}
	if estimatedItems <= 0 {
		estimatedItems = 1
	}
	//define the percentage of window cache, here is 1%
	const lruPct = 1
	//calculate the capacity of window cache
	lruSz := max((lruPct*estimatedItems)/100, 1)

	//calculate the capacity of LFU cache
	slruSz := max(int(float64(estimatedItems)*((100-lruPct)/100.0)), 1)

	//LFU is divided into two parts, stageOne accounts for 20%
	slruO := max(int(0.2*float64(slruSz)), 1)

	data := make(map[uint64]*list.Element, estimatedItems)

	return &Cache{
		lru:       newWindowLRU(lruSz, data),
		slru:      newSLRU(data, slruO, slruSz-slruO),
		door:      newFilter(estimatedItems, 0.01), //set the error rate of bloom filter to 0.01
		c:         newCmSketch(int64(estimatedItems)),
		data:      data, //share the same map to store data
		threshold: int32(estimatedItems * 10),
		maxCost:   maxCost,
		costFn:    costFn,
	}
}

func (c *Cache) Set(key any, value any) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) set(key, value any) bool {
	// keyHash is used to quickly locate, conflict is used to determine conflicts
	keyHash, conflictHash := c.keyToHash(key)
	cost := c.entryCost(value)
	if cost > c.maxCost {
		if elem, ok := c.data[keyHash]; ok {
			item := elem.Value.(*storeItem)
			if item.conflict == conflictHash {
				c.removeElement(elem)
			}
		}
		return false
	}

	if elem, ok := c.data[keyHash]; ok {
		item := elem.Value.(*storeItem)
		if item.conflict == conflictHash {
			c.usedCost += cost - item.cost
			item.value = value
			item.cost = cost
			if item.stage == 0 {
				c.lru.get(elem)
			} else {
				c.slru.get(elem)
			}
			c.trimToBudget()
			return true
		}
	}

	// the newly added cache is placed in window lru, so stage = 0
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
		cost:     cost,
	}

	// if window is full, return the evicted data
	eitem, evicted := c.lru.add(i)

	if !evicted {
		c.usedCost += cost
		c.trimToBudget()
		return true
	}
	c.usedCost += cost - eitem.cost

	// if there is an evicted data in window, it will come here
	// need to find a victim from stageOne of LFU
	// and then PK
	victim := c.slru.victim()

	// come here because LFU is not full, so the evicted data in window can enter stageOne
	if victim == nil {
		if removed, ok := c.slru.add(eitem); ok {
			c.usedCost -= removed.cost
		}
		c.usedCost += eitem.cost
		c.trimToBudget()
		return true
	}

	// PK, must appear in bloomfilter once, then allow PK
	// appear in bf, means access frequency >= 2
	if keyHash <= math.MaxUint32 {
		if !c.door.Allow(uint32(eitem.key)) {
			return true
		}
	}

	// estimate the access frequency of the evicted data in windowlru and LFU
	// the one with higher access frequency is more likely to be retained
	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(eitem.key)

	if ocount < vcount {
		c.trimToBudget()
		return true
	}

	// the one who stays enters stageOne
	if removed, ok := c.slru.add(eitem); ok {
		c.usedCost -= removed.cost
	}
	c.usedCost += eitem.cost
	c.trimToBudget()
	return true
}

func (c *Cache) Get(key any) (any, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

func (c *Cache) get(key any) (any, bool) {
	c.t++
	if c.t == c.threshold {
		c.c.Reset()
		c.door.reset()
		c.t = 0
	}

	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		if keyHash <= math.MaxUint32 {
			c.door.Allow(uint32(keyHash))
		}
		c.c.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)

	if item.conflict != conflictHash {
		if keyHash <= math.MaxUint32 {
			c.door.Allow(uint32(keyHash))
		}
		c.c.Increment(keyHash)
		return nil, false
	}
	if keyHash <= math.MaxUint32 {
		c.door.Allow(uint32(keyHash))
	}
	c.c.Increment(item.key)

	v := item.value

	if item.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}

	return v, true

}

func (c *Cache) Del(key any) (any, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key any) (any, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)

	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}

	c.removeElement(val)
	return item.conflict, true
}

func (c *Cache) entryCost(value any) int64 {
	if c.costFn == nil {
		return 1
	}
	cost := c.costFn(value)
	if cost <= 0 {
		return 1
	}
	return cost
}

func (c *Cache) removeElement(elem *list.Element) {
	if elem == nil {
		return
	}
	item := elem.Value.(*storeItem)
	if item.stage == 0 {
		c.lru.remove(elem)
	} else {
		c.slru.remove(elem)
	}
	c.usedCost -= item.cost
	if c.usedCost < 0 {
		c.usedCost = 0
	}
}

func (c *Cache) trimToBudget() {
	for c.usedCost > c.maxCost {
		if item, ok := c.slru.removeStageOneOldest(); ok {
			c.usedCost -= item.cost
			continue
		}
		if item, ok := c.lru.removeOldest(); ok {
			c.usedCost -= item.cost
			continue
		}
		if item, ok := c.slru.removeStageTwoOldest(); ok {
			c.usedCost -= item.cost
			continue
		}
		break
	}
	if c.usedCost < 0 {
		c.usedCost = 0
	}
}

func (c *Cache) keyToHash(key any) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
