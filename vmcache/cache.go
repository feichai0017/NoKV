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
	// PageSize controls the size of each slot in bytes. Typically matches the
	// SSTable block size.
	PageSize int
	// Capacity is the total number of pages that can be resident.
	Capacity int
	// EvictBatch limits how many candidates a single eviction pass scans. Zero
	// defaults to Capacity.
	EvictBatch int
	// UseMmap toggles whether the cache allocates via anonymous mmap. If false,
	// a Go heap slice is used instead.
	UseMmap bool
	// DisableMadvise skips MADV_DONTNEED on eviction.
	DisableMadvise bool
	// Flusher, if provided, is invoked before evicting a dirty page. Returning
	// an error keeps the page resident and marks it as recently used.
	Flusher func(id uint64, data []byte) error
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

type frameMeta struct {
	recent  atomic.Bool
	loading atomic.Bool
	dirty   atomic.Bool
	size    atomic.Int32
	ready   chan struct{}
	err     atomic.Pointer[error]
	value   atomic.Value
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

// Release drops the reference to the underlying page, allowing eviction once
// no other pins exist.
func (h *Handle) Release(dirty bool) {
	if h == nil || h.cache == nil {
		return
	}
	if !h.released.CompareAndSwap(false, true) {
		return
	}
	h.cache.release(h.id, dirty)
}

// Value returns cached metadata associated with this page.
func (h *Handle) Value() any {
	if h == nil || h.cache == nil {
		return nil
	}
	return h.cache.value(h.id)
}

// SetValue attaches metadata to this page.
func (h *Handle) SetValue(v any) {
	if h == nil || h.cache == nil {
		return
	}
	h.cache.setValue(h.id, v)
}

const invalidID = ^uint64(0)

var errClosed = errors.New("vmcache: cache closed")

// Cache manages a region of anonymous memory and exposes page-level pins.
type Cache struct {
	mu     sync.RWMutex
	loader Loader
	opts   Options

	region []byte

	resident *residentSet
	slotID   []uint64 // slot -> pageID
	meta     []frameMeta
	state    []pageState

	freeSlots  []int
	clock      []int // slots
	clockIndex []int
	hand       int

	releaseCh chan struct{}
	closed    bool

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

// New allocates the backing region and returns a VMCache instance.
func New(opts Options, loader Loader) (*Cache, error) {
	if opts.PageSize <= 0 {
		return nil, errors.New("vmcache: PageSize must be positive")
	}
	if opts.Capacity <= 0 {
		return nil, errors.New("vmcache: Capacity must be positive")
	}
	regionSize := opts.PageSize * opts.Capacity
	if regionSize <= 0 {
		return nil, fmt.Errorf("vmcache: invalid region size %d", regionSize)
	}
	var region []byte
	var err error
	if opts.UseMmap {
		region, err = unix.Mmap(-1, 0, regionSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE|unix.MAP_NORESERVE)
		if err != nil {
			return nil, fmt.Errorf("vmcache: mmap failed: %w", err)
		}
	} else {
		region = make([]byte, regionSize)
	}
	free := make([]int, opts.Capacity)
	for i := range free {
		free[i] = opts.Capacity - 1 - i
	}
	if opts.EvictBatch <= 0 {
		opts.EvictBatch = opts.Capacity
	}
	slotID := make([]uint64, opts.Capacity)
	for i := range slotID {
		slotID[i] = invalidID
	}
	state := make([]pageState, opts.Capacity)
	for i := range state {
		state[i].init()
	}
	clockIndex := make([]int, opts.Capacity)
	for i := range clockIndex {
		clockIndex[i] = -1
	}

	return &Cache{
		loader:     loader,
		opts:       opts,
		region:     region,
		resident:   newResident(opts.Capacity),
		slotID:     slotID,
		meta:       make([]frameMeta, opts.Capacity),
		state:      state,
		freeSlots:  free,
		clock:      make([]int, 0, opts.Capacity),
		clockIndex: clockIndex,
		releaseCh:  make(chan struct{}, opts.Capacity),
	}, nil
}

// Close releases resources. It does not wait for in-flight loads.
func (c *Cache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	region := c.region
	useMmap := c.opts.UseMmap
	c.mu.Unlock()
	if useMmap {
		return unix.Munmap(region)
	}
	return nil
}

// Fix pins the page for id, loading it on-demand via Loader. The returned bool
// indicates whether the page was already resident.
func (c *Cache) Fix(id uint64) (*Handle, bool, error) {
	for {
		slot, size, hit, meta, err := c.allocateSlot(id)
		if err != nil {
			return nil, false, err
		}
		if hit {
			return c.makeHandle(id, slot, size), true, nil
		}
		if c.loader == nil {
			c.releaseSlot(slot, id, meta, fmt.Errorf("vmcache: no loader configured"))
			return nil, false, errors.New("vmcache: no loader configured")
		}
		start := time.Now()
		n, loadErr := c.loader(id, c.page(slot))
		c.stats.loadNanos.Add(uint64(time.Since(start).Nanoseconds()))
		c.stats.loads.Add(1)
		if loadErr != nil || n <= 0 || n > c.opts.PageSize {
			if loadErr == nil {
				loadErr = fmt.Errorf("vmcache: loader wrote invalid size %d", n)
			}
			c.releaseSlot(slot, id, meta, loadErr)
			return nil, false, loadErr
		}
		c.completeSlot(slot, n, meta)
		if !c.pinShared(slot) {
			c.releaseSlot(slot, id, meta, errors.New("vmcache: failed to pin loaded page"))
			return nil, false, errors.New("vmcache: failed to pin loaded page")
		}
		return c.makeHandle(id, slot, n), false, nil
	}
}

// Prefetch triggers asynchronous loads of the provided IDs.
func (c *Cache) Prefetch(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > len(ids) {
		workers = len(ids)
	}
	ch := make(chan uint64, len(ids))
	for _, id := range ids {
		ch <- id
	}
	close(ch)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for id := range ch {
				h, _, err := c.Fix(id)
				if err == nil && h != nil {
					h.Release(false)
				}
			}
		}()
	}
	wg.Wait()
}

// Release decrements the refcount without needing a Handle.
func (c *Cache) Release(id uint64, dirty bool) {
	c.release(id, dirty)
}

func (c *Cache) release(id uint64, dirty bool) {
	slot, ok := c.lookupSlot(id)
	if !ok {
		return
	}
	meta := &c.meta[slot]
	if dirty {
		meta.dirty.Store(true)
	}
	ps := &c.state[slot]
	ps.unlockS()
	meta.recent.Store(false)
	c.notifyWaiters()
}

// Evict removes specific pages and returns their slots to the free list.
func (c *Cache) Evict(ids []uint64) {
	c.mu.Lock()
	for _, id := range ids {
		slot, ok := c.lookupSlot(id)
		if !ok {
			continue
		}
		meta := &c.meta[slot]
		if meta.loading.Load() || c.state[slot].state(c.state[slot].load()) > stateUnlocked {
			continue
		}
		if c.opts.Flusher != nil && meta.dirty.Load() {
			if err := c.opts.Flusher(id, c.page(slot)[:int(meta.size.Load())]); err != nil {
				meta.recent.Store(true)
				continue
			}
			meta.dirty.Store(false)
		}
		c.dropSlotLocked(slot)
	}
	c.mu.Unlock()
	c.notifyWaiters()
}

// Stats returns a snapshot of counters.
func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Stats{
		Hits:      c.stats.hits.Load(),
		Misses:    c.stats.misses.Load(),
		Evictions: c.stats.evictions.Load(),
		Resident:  uint64(c.residentLen()),
		Loads:     c.stats.loads.Load(),
		LoadNanos: c.stats.loadNanos.Load(),
		Waits:     c.stats.waits.Load(),
		WaitNanos: c.stats.waitNanos.Load(),
		Madvises:  c.stats.madvises.Load(),
	}
}

func (c *Cache) allocateSlot(id uint64) (slot int, size int, hit bool, meta *frameMeta, err error) {
	for {
		if existing, ok := c.lookupSlot(id); ok {
			if h, size, ok := c.fixExisting(existing); ok {
				c.stats.hits.Add(1)
				return existing, size, true, h, nil
			}
			continue
		}

		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return 0, 0, false, nil, errClosed
		}
		if existing, ok := c.lookupSlot(id); ok {
			if h, size, ok := c.fixExisting(existing); ok {
				c.stats.hits.Add(1)
				c.mu.Unlock()
				return existing, size, true, h, nil
			}
			continue
		}

		slot, err := c.grabSlotLocked()
		if err != nil {
			c.mu.Unlock()
			return 0, 0, false, nil, err
		}
		m := &c.meta[slot]
		m.recent.Store(true)
		m.loading.Store(true)
		m.ready = make(chan struct{})
		var errNil error
		m.err.Store(&errNil)
		m.size.Store(0)
		ps := &c.state[slot]
		for {
			old := ps.load()
			if ps.state(old) == stateEvicted {
				if ps.tryLockX(old) {
					break
				}
			} else if ps.state(old) == stateUnlocked || ps.state(old) == stateMarked {
				if ps.tryLockX(old) {
					break
				}
			} else {
				c.dropSlotLocked(slot)
				c.mu.Unlock()
				continue
			}
		}

		c.setSlot(id, slot)
		c.slotID[slot] = id
		c.clockIndex[slot] = len(c.clock)
		c.clock = append(c.clock, slot)
		c.stats.misses.Add(1)
		c.mu.Unlock()
		return slot, 0, false, m, nil
	}
}

func (c *Cache) fixExisting(slot int) (*frameMeta, int, bool) {
	ps := &c.state[slot]
	meta := &c.meta[slot]
	for {
		v := ps.load()
		state := ps.state(v)
		switch state {
		case stateEvicted:
			if ps.tryLockX(v) {
				if err := c.handleFault(slot); err != nil {
					return nil, 0, false
				}
				ps.unlockX()
				if !c.pinShared(slot) {
					return nil, 0, false
				}
				meta.recent.Store(true)
				return meta, int(meta.size.Load()), true
			}
		case stateLocked:
			runtime.Gosched()
		case stateMarked, stateUnlocked:
			if ps.tryLockS(v) {
				meta.recent.Store(true)
				return meta, int(meta.size.Load()), true
			}
		default:
			runtime.Gosched()
		}
	}
}

func (c *Cache) handleFault(slot int) error {
	id := c.slotID[slot]
	meta := &c.meta[slot]
	if meta.ready == nil {
		meta.ready = make(chan struct{})
	}
	meta.loading.Store(true)
	var errNil error
	meta.err.Store(&errNil)
	meta.size.Store(0)
	start := time.Now()
	n, err := c.loader(id, c.page(slot))
	c.stats.loadNanos.Add(uint64(time.Since(start).Nanoseconds()))
	c.stats.loads.Add(1)
	if err != nil {
		meta.err.Store(&err)
		meta.loading.Store(false)
		close(meta.ready)
		return err
	}
	meta.size.Store(int32(n))
	meta.loading.Store(false)
	close(meta.ready)
	return nil
}

func (c *Cache) pinShared(slot int) bool {
	ps := &c.state[slot]
	for {
		v := ps.load()
		s := ps.state(v)
		switch s {
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

func (c *Cache) completeSlot(slot, size int, meta *frameMeta) {
	c.mu.Lock()
	meta.size.Store(int32(size))
	meta.loading.Store(false)
	var errNil error
	meta.err.Store(&errNil)
	close(meta.ready)
	c.state[slot].unlockX()
	c.mu.Unlock()
	c.notifyWaiters()
}

func (c *Cache) releaseSlot(slot int, id uint64, meta *frameMeta, err error) {
	c.mu.Lock()
	if meta.ready == nil {
		meta.ready = make(chan struct{})
	}
	if err != nil {
		meta.err.Store(&err)
	} else {
		var errNil error
		meta.err.Store(&errNil)
	}
	meta.loading.Store(false)
	close(meta.ready)
	c.state[slot].unlockXEvicted()
	c.dropSlotLocked(slot)
	c.mu.Unlock()
	c.notifyWaiters()
}

func (c *Cache) grabSlotLocked() (int, error) {
	for len(c.freeSlots) == 0 {
		c.evictLocked()
		if len(c.freeSlots) == 0 {
			c.mu.Unlock()
			wait := c.waitForRelease()
			if wait > 0 {
				c.stats.waits.Add(1)
				c.stats.waitNanos.Add(uint64(wait))
			}
			c.mu.Lock()
			if c.closed {
				return 0, errClosed
			}
		}
	}
	last := len(c.freeSlots) - 1
	slot := c.freeSlots[last]
	c.freeSlots = c.freeSlots[:last]
	return slot, nil
}

func (c *Cache) evictLocked() {
	if len(c.clock) == 0 {
		return
	}
	budget := c.opts.EvictBatch
	if budget <= 0 || budget > len(c.clock) {
		budget = len(c.clock)
	}
	scanned := 0
	for len(c.freeSlots) == 0 && scanned < budget && len(c.clock) > 0 {
		if c.hand >= len(c.clock) {
			c.hand = 0
		}
		slot := c.clock[c.hand]
		c.hand++
		meta := &c.meta[slot]
		if meta.loading.Load() || c.state[slot].state(c.state[slot].load()) > stateUnlocked {
			scanned++
			continue
		}
		if meta.recent.Swap(false) {
			scanned++
			continue
		}
		if c.opts.Flusher != nil && meta.dirty.Load() {
			if err := c.opts.Flusher(c.slotID[slot], c.page(slot)[:int(meta.size.Load())]); err != nil {
				meta.recent.Store(true)
				scanned++
				continue
			}
			meta.dirty.Store(false)
		}
		c.dropSlotLocked(slot)
		c.stats.evictions.Add(1)
		scanned++
	}
}

func (c *Cache) dropSlotLocked(slot int) {
	id := c.slotID[slot]
	if id != invalidID {
		c.delSlot(id)
		c.slotID[slot] = invalidID
	}
	if idx := c.clockIndex[slot]; idx >= 0 {
		last := len(c.clock) - 1
		lastSlot := c.clock[last]
		c.clock[idx] = lastSlot
		c.clock = c.clock[:last]
		c.clockIndex[slot] = -1
		if idx < len(c.clock) {
			c.clockIndex[lastSlot] = idx
		}
		if c.hand > idx {
			c.hand--
		}
		if c.hand >= len(c.clock) {
			c.hand = 0
		}
	}
	if !c.opts.DisableMadvise && c.opts.UseMmap {
		if err := unix.Madvise(c.page(slot), unix.MADV_DONTNEED); err == nil {
			c.stats.madvises.Add(1)
		}
	}
	c.meta[slot].recent.Store(false)
	c.meta[slot].dirty.Store(false)
	c.meta[slot].loading.Store(false)
	c.meta[slot].size.Store(0)
	c.freeSlots = append(c.freeSlots, slot)
}

func (c *Cache) waitForRelease() time.Duration {
	start := time.Now()
	select {
	case <-c.releaseCh:
	case <-time.After(time.Millisecond):
		runtime.Gosched()
	}
	return time.Since(start)
}

func (c *Cache) notifyWaiters() {
	select {
	case c.releaseCh <- struct{}{}:
	default:
	}
}

func (c *Cache) makeHandle(id uint64, slot, size int) *Handle {
	if size <= 0 {
		size = c.opts.PageSize
	}
	return &Handle{
		cache: c,
		id:    id,
		slot:  slot,
		size:  size,
	}
}

func (c *Cache) value(id uint64) any {
	if slot, ok := c.lookupSlot(id); ok {
		return c.meta[slot].value.Load()
	}
	return nil
}

func (c *Cache) setValue(id uint64, v any) {
	if slot, ok := c.lookupSlot(id); ok {
		c.meta[slot].value.Store(v)
	}
}

func (c *Cache) page(slot int) []byte {
	start := slot * c.opts.PageSize
	end := start + c.opts.PageSize
	return c.region[start:end]
}

func (c *Cache) lookupSlot(id uint64) (int, bool) {
	return c.resident.get(id)
}

func (c *Cache) setSlot(id uint64, slot int) {
	c.resident.insert(id, slot)
}

func (c *Cache) delSlot(id uint64) {
	c.resident.remove(id)
}

func (c *Cache) residentLen() int {
	return c.resident.len()
}
