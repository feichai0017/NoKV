package vmcache

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Loader fills dst with the bytes for the given page ID and returns how many
// bytes were written. Returning an error aborts the Fix call.
type Loader func(id uint64, dst []byte) (int, error)

// Options configure the VM-backed cache.
type Options struct {
	PageSize       int
	Capacity       int
	EvictBatch     int
	UseMmap        bool
	DisableMadvise bool
	Shards         int
	Flusher        func(id uint64, data []byte) error
}

// Stats exposes basic hit/miss and residency counters.
type Stats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Resident  uint64
	Loads     uint64
	LoadNanos uint64
	Waits     uint64
	WaitNanos uint64
	Madvises  uint64
}

// Handle pins a cached page for the caller.
type Handle struct {
	cache    *Cache
	id       uint64
	slot     int
	size     int
	released atomic.Bool
}

// Bytes returns the populated portion of the page.
func (h *Handle) Bytes() []byte {
	if h == nil || h.cache == nil {
		return nil
	}
	return h.cache.page(h.slot)[:h.size]
}

// Release drops the reference to the underlying page.
func (h *Handle) Release(dirty bool) {
	if h == nil || h.cache == nil {
		return
	}
	if !h.released.CompareAndSwap(false, true) {
		return
	}
	h.cache.release(h.id, dirty)
}

// Cache is a sharded VM-backed cache with per-shard CLOCK eviction.
type Cache struct {
	loader Loader
	opts   Options

	region []byte

	shards []*shard

	stats struct {
		hits      atomic.Uint64
		misses    atomic.Uint64
		evictions atomic.Uint64
		loads     atomic.Uint64
		loadNanos atomic.Uint64
		waitNanos atomic.Uint64
		waits     atomic.Uint64
		madvises  atomic.Uint64
	}
}

type frameMeta struct {
	recent  atomic.Bool
	loading atomic.Bool
	dirty   atomic.Bool
	size    atomic.Int32
	ready   chan struct{}
	err     atomic.Pointer[error]
	value   atomic.Value
}

type shard struct {
	mu         sync.Mutex
	baseSlot   int
	capacity   int
	slotID     []uint64
	state      []pageState
	meta       []frameMeta
	idToSlot   map[uint64]int
	freeSlots  []int
	clock      []int
	clockIndex []int
	hand       int
}

const invalidID = ^uint64(0)

// New allocates the backing region and returns a VMCache instance.
func New(opts Options, loader Loader) (*Cache, error) {
	if opts.PageSize <= 0 {
		return nil, errors.New("vmcache: PageSize must be positive")
	}
	if opts.Capacity <= 0 {
		return nil, errors.New("vmcache: Capacity must be positive")
	}
	if opts.Shards <= 0 {
		opts.Shards = runtime.GOMAXPROCS(0)
		if opts.Shards < 1 {
			opts.Shards = 1
		}
	}
	if opts.EvictBatch <= 0 {
		opts.EvictBatch = opts.Capacity
	}
	regionSize := opts.PageSize * opts.Capacity
	region, err := unix.Mmap(-1, 0, regionSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE|unix.MAP_NORESERVE)
	if err != nil {
		return nil, fmt.Errorf("vmcache: mmap failed: %w", err)
	}

	c := &Cache{
		loader: loader,
		opts:   opts,
		region: region,
	}
	perShard := opts.Capacity / opts.Shards
	rem := opts.Capacity % opts.Shards
	slotBase := 0
	for i := 0; i < opts.Shards; i++ {
		sz := perShard
		if i < rem {
			sz++
		}
		s := newShard(slotBase, sz, opts)
		c.shards = append(c.shards, s)
		slotBase += sz
	}
	return c, nil
}

func newShard(base, cap int, opts Options) *shard {
	slotID := make([]uint64, cap)
	for i := range slotID {
		slotID[i] = invalidID
	}
	state := make([]pageState, cap)
	for i := range state {
		state[i].init()
	}
	free := make([]int, cap)
	for i := range free {
		free[i] = base + cap - 1 - i
	}
	clockIndex := make([]int, cap)
	for i := range clockIndex {
		clockIndex[i] = -1
	}
	return &shard{
		baseSlot:   base,
		capacity:   cap,
		slotID:     slotID,
		state:      state,
		meta:       make([]frameMeta, cap),
		idToSlot:   make(map[uint64]int, cap),
		freeSlots:  free,
		clock:      make([]int, 0, cap),
		clockIndex: clockIndex,
	}
}

func (c *Cache) shardFor(id uint64) *shard {
	if len(c.shards) == 1 {
		return c.shards[0]
	}
	return c.shards[hash64(id)&uint64(len(c.shards)-1)]
}

// Fix pins the page for id, loading it on-demand via Loader.
func (c *Cache) Fix(id uint64) (*Handle, bool, error) {
	s := c.shardFor(id)
	h, hit, err := c.fixShard(s, id)
	return h, hit, err
}

func (c *Cache) fixShard(s *shard, id uint64) (*Handle, bool, error) {
	for {
		if slot, ok := s.lookup(id); ok {
			if size, ok := c.fixExisting(s, slot); ok {
				c.stats.hits.Add(1)
				return &Handle{cache: c, id: id, slot: slot, size: size}, true, nil
			}
			continue
		}

		slot, err := c.allocateSlot(s, id)
		if err != nil {
			return nil, false, err
		}
		// load under exclusive lock
		start := time.Now()
		n, loadErr := c.loader(id, c.page(slot))
		c.stats.loads.Add(1)
		c.stats.loadNanos.Add(uint64(time.Since(start).Nanoseconds()))
		s.mu.Lock()
		meta := &s.meta[slot-s.baseSlot]
		if loadErr != nil || n <= 0 || n > c.opts.PageSize {
			if loadErr == nil {
				loadErr = fmt.Errorf("vmcache: invalid load size %d", n)
			}
			meta.err.Store(&loadErr)
			meta.loading.Store(false)
			close(meta.ready)
			s.removeSlot(slot)
			s.mu.Unlock()
			return nil, false, loadErr
		}
		meta.size.Store(int32(n))
		meta.loading.Store(false)
		var errNil error
		meta.err.Store(&errNil)
		close(meta.ready)
		s.unlockX(slot, false)
		if !s.tryShared(slot) {
			s.removeSlot(slot)
			s.mu.Unlock()
			return nil, false, errors.New("vmcache: failed to pin loaded page")
		}
		meta.recent.Store(true)
		s.mu.Unlock()
		return &Handle{cache: c, id: id, slot: slot, size: n}, false, nil
	}
}

// Release decrements the refcount without needing a Handle.
func (c *Cache) Release(id uint64, dirty bool) {
	c.release(id, dirty)
}

func (c *Cache) release(id uint64, dirty bool) {
	s := c.shardFor(id)
	if s == nil {
		return
	}
	s.mu.Lock()
	slot, ok := s.idToSlot[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	meta := &s.meta[slot-s.baseSlot]
	if dirty {
		meta.dirty.Store(true)
	}
	s.unlockS(slot)
	meta.recent.Store(false)
	s.mu.Unlock()
}

// Evict removes specific pages.
func (c *Cache) Evict(ids []uint64) {
	shardBuckets := make(map[*shard][]uint64)
	for _, id := range ids {
		shardBuckets[c.shardFor(id)] = append(shardBuckets[c.shardFor(id)], id)
	}
	for sh, list := range shardBuckets {
		sh.evictList(list, c)
	}
}

// Prefetch triggers asynchronous loads.
func (c *Cache) Prefetch(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	go func() {
		for _, id := range ids {
			h, _, err := c.Fix(id)
			if err == nil && h != nil {
				h.Release(false)
			}
		}
	}()
}

// Stats returns a snapshot of counters.
func (c *Cache) Stats() Stats {
	return Stats{
		Hits:      c.stats.hits.Load(),
		Misses:    c.stats.misses.Load(),
		Evictions: c.stats.evictions.Load(),
		Resident:  c.residentLen(),
		Loads:     c.stats.loads.Load(),
		LoadNanos: c.stats.loadNanos.Load(),
		Waits:     c.stats.waits.Load(),
		WaitNanos: c.stats.waitNanos.Load(),
		Madvises:  c.stats.madvises.Load(),
	}
}

// Close unmaps the backing region.
func (c *Cache) Close() error {
	return unix.Munmap(c.region)
}

// --- shard operations ---

func (s *shard) lookup(id uint64) (int, bool) {
	s.mu.Lock()
	slot, ok := s.idToSlot[id]
	s.mu.Unlock()
	return slot, ok
}

func (c *Cache) allocateSlot(s *shard, id uint64) (int, error) {
	for {
		s.mu.Lock()
		if slot, ok := s.idToSlot[id]; ok {
			s.mu.Unlock()
			return slot, nil
		}
		if len(s.freeSlots) == 0 {
			s.evictSome(c)
			if len(s.freeSlots) == 0 {
				s.mu.Unlock()
				time.Sleep(time.Millisecond)
				continue
			}
		}
		last := len(s.freeSlots) - 1
		slot := s.freeSlots[last]
		s.freeSlots = s.freeSlots[:last]
		s.idToSlot[id] = slot
		local := slot - s.baseSlot
		s.slotID[local] = id
		m := &s.meta[local]
		m.loading.Store(true)
		m.ready = make(chan struct{})
		var errNil error
		m.err.Store(&errNil)
		m.recent.Store(true)
		s.clockIndex[local] = len(s.clock)
		s.clock = append(s.clock, slot)
		s.mu.Unlock()
		c.stats.misses.Add(1)
		return slot, nil
	}
}

func (c *Cache) fixExisting(s *shard, slot int) (int, bool) {
	local := slot - s.baseSlot
	ps := &s.state[local]
	meta := &s.meta[local]
	for {
		v := ps.load()
		switch ps.state(v) {
		case stateEvicted:
			if ps.tryLockX(v) {
				if err := c.handleFaultLocked(s, slot); err != nil {
					s.unlockX(slot, true)
					return 0, false
				}
				s.unlockX(slot, false)
				if !s.tryShared(slot) {
					return 0, false
				}
				meta.recent.Store(true)
				return int(meta.size.Load()), true
			}
		case stateLocked:
			runtime.Gosched()
		default:
			if ps.tryLockS(v) {
				meta.recent.Store(true)
				return int(meta.size.Load()), true
			}
			runtime.Gosched()
		}
	}
}

func (c *Cache) handleFaultLocked(s *shard, slot int) error {
	id := s.slotID[slot-s.baseSlot]
	meta := &s.meta[slot-s.baseSlot]
	start := time.Now()
	n, err := c.loader(id, c.page(slot))
	c.stats.loads.Add(1)
	c.stats.loadNanos.Add(uint64(time.Since(start).Nanoseconds()))
	if err != nil {
		meta.err.Store(&err)
		meta.loading.Store(false)
		close(meta.ready)
		return err
	}
	meta.size.Store(int32(n))
	meta.loading.Store(false)
	var errNil error
	meta.err.Store(&errNil)
	close(meta.ready)
	return nil
}

func (s *shard) unlockX(slot int, evict bool) {
	local := slot - s.baseSlot
	ps := &s.state[local]
	if evict {
		ps.unlockXEvicted()
	} else {
		ps.unlockX()
	}
}

func (s *shard) unlockS(slot int) {
	local := slot - s.baseSlot
	s.state[local].unlockS()
}

func (s *shard) tryShared(slot int) bool {
	local := slot - s.baseSlot
	ps := &s.state[local]
	for {
		v := ps.load()
		switch ps.state(v) {
		case stateEvicted:
			return false
		case stateLocked:
			runtime.Gosched()
		default:
			if ps.tryLockS(v) {
				return true
			}
			runtime.Gosched()
		}
	}
}

func (s *shard) evictSome(c *Cache) {
	if len(s.clock) == 0 {
		return
	}
	budget := c.opts.EvictBatch
	if budget > len(s.clock) {
		budget = len(s.clock)
	}
	scanned := 0
	for len(s.freeSlots) == 0 && scanned < budget && len(s.clock) > 0 {
		if s.hand >= len(s.clock) {
			s.hand = 0
		}
		slot := s.clock[s.hand]
		local := slot - s.baseSlot
		s.hand++
		ps := &s.state[local]
		meta := &s.meta[local]
		state := ps.state(ps.load())
		if state == stateLocked || state == stateEvicted {
			scanned++
			continue
		}
		if meta.loading.Load() {
			scanned++
			continue
		}
		if meta.recent.Swap(false) {
			scanned++
			continue
		}
		if c.opts.Flusher != nil && meta.dirty.Load() {
			if err := c.opts.Flusher(s.slotID[local], c.pageFor(slot, int(meta.size.Load()))); err != nil {
				meta.recent.Store(true)
				scanned++
				continue
			}
			meta.dirty.Store(false)
		}
		s.removeSlot(slot)
		c.stats.evictions.Add(1)
		scanned++
	}
}

func (s *shard) evictList(ids []uint64, c *Cache) {
	s.mu.Lock()
	for _, id := range ids {
		slot, ok := s.idToSlot[id]
		if !ok {
			continue
		}
		local := slot - s.baseSlot
		meta := &s.meta[local]
		if meta.loading.Load() {
			continue
		}
		s.removeSlot(slot)
		c.stats.evictions.Add(1)
	}
	s.mu.Unlock()
}

func (s *shard) removeSlot(slot int) {
	local := slot - s.baseSlot
	id := s.slotID[local]
	delete(s.idToSlot, id)
	s.slotID[local] = invalidID
	s.state[local].unlockXEvicted()
	if s.clockIndex[local] >= 0 {
		idx := s.clockIndex[local]
		last := len(s.clock) - 1
		s.clock[idx] = s.clock[last]
		s.clock = s.clock[:last]
		if idx < len(s.clock) {
			otherLocal := s.clock[idx] - s.baseSlot
			s.clockIndex[otherLocal] = idx
		}
		s.clockIndex[local] = -1
	}
	s.meta[local].recent.Store(false)
	s.meta[local].dirty.Store(false)
	s.meta[local].loading.Store(false)
	s.meta[local].size.Store(0)
	s.freeSlots = append(s.freeSlots, slot)
}

func cBool(b bool) bool { return b }

// helpers
func (c *Cache) page(slot int) []byte {
	start := slot * c.opts.PageSize
	end := start + c.opts.PageSize
	return c.region[start:end]
}

func (c *Cache) residentLen() uint64 {
	var total uint64
	for _, s := range c.shards {
		total += uint64(len(s.idToSlot))
	}
	return total
}

func (c *Cache) pageFor(slot int, size int) []byte {
	if size <= 0 || size > c.opts.PageSize {
		size = c.opts.PageSize
	}
	return c.page(slot)[:size]
}

func hash64(k uint64) uint64 {
	const m = uint64(0xc6a4a7935bd1e995)
	const r = uint64(47)
	h := uint64(0x8445d61a4e774912) ^ uint64(0x9e3779b97f4a7c15)
	k *= m
	k ^= k >> r
	k *= m
	h ^= k
	h *= m
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}
