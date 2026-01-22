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
}

// NewCache size means the number of data to be cached
func NewCache(size int) *Cache {
	//define the percentage of window cache, here is 1%
	const lruPct = 1
	//calculate the capacity of window cache
	lruSz := max((lruPct*size)/100, 1)

	//calculate the capacity of LFU cache
	slruSz := max(int(float64(size)*((100-lruPct)/100.0)), 1)

	//LFU is divided into two parts, stageOne accounts for 20%
	slruO := max(int(0.2*float64(slruSz)), 1)

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slruO, slruSz-slruO),
		door: newFilter(size, 0.01), //set the error rate of bloom filter to 0.01
		c:    newCmSketch(int64(size)),
		data: data, //share the same map to store data
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

	// the newly added cache is placed in window lru, so stage = 0
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// if window is full, return the evicted data
	eitem, evicted := c.lru.add(i)

	if !evicted {
		return true
	}

	// if there is an evicted data in window, it will come here
	// need to find a victim from stageOne of LFU
	// and then PK
	victim := c.slru.victim()

	// come here because LFU is not full, so the evicted data in window can enter stageOne
	if victim == nil {
		c.slru.add(eitem)
		return true
	}

	// PK, must appear in bloomfilter once, then allow PK
	// appear in bf, means access frequency >= 2
	if !c.door.Allow(uint32(eitem.key)) {
		return true
	}

	// estimate the access frequency of the evicted data in windowlru and LFU
	// the one with higher access frequency is more likely to be retained
	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(eitem.key)

	if ocount < vcount {
		return true
	}

	// the one who stays enters stageOne
	c.slru.add(eitem)
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
	c.c.Increment(uint64(item.key))

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

	delete(c.data, keyHash)
	return item.conflict, true
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
