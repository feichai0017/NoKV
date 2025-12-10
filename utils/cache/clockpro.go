package cache

import "sync"

// ClockProCache is a tiny CLOCK-style cache keyed by uint64 with a ref bit.
// It is intended for small hot sets (e.g. hot Bloom/Index) and uses a single
// lock for simplicity.
type ClockProCache[V any] struct {
	mu      sync.Mutex
	cap     int
	hand    int
	entries []clockEntry[V]
	index   map[uint64]int
}

type clockEntry[V any] struct {
	key uint64
	val V
	ref bool
}

func NewClockProCache[V any](capacity int) *ClockProCache[V] {
	if capacity <= 0 {
		return nil
	}
	return &ClockProCache[V]{
		cap:   capacity,
		index: make(map[uint64]int, capacity),
	}
}

func (c *ClockProCache[V]) Get(key uint64) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	idx, ok := c.index[key]
	if !ok || idx >= len(c.entries) {
		return zero, false
	}
	e := &c.entries[idx]
	e.ref = true
	return e.val, true
}

func (c *ClockProCache[V]) Promote(key uint64, val V) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx, ok := c.index[key]; ok && idx < len(c.entries) {
		c.entries[idx].val = val
		c.entries[idx].ref = true
		return
	}
	if len(c.entries) < c.cap {
		c.entries = append(c.entries, clockEntry[V]{key: key, val: val, ref: true})
		c.index[key] = len(c.entries) - 1
		return
	}
	// Evict via CLOCK hand.
	for steps := 0; steps < c.cap*2; steps++ {
		e := &c.entries[c.hand]
		if e.ref {
			e.ref = false
			c.hand = (c.hand + 1) % c.cap
			continue
		}
		delete(c.index, e.key)
		*e = clockEntry[V]{key: key, val: val, ref: true}
		c.index[key] = c.hand
		c.hand = (c.hand + 1) % c.cap
		return
	}
}

// Delete removes a key from the cache if present. It leaves the slot in-place
// to avoid reshuffling indexes; future promotions will overwrite it.
func (c *ClockProCache[V]) Delete(key uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	idx, ok := c.index[key]
	if !ok || idx >= len(c.entries) {
		return
	}
	delete(c.index, key)
	var zero V
	c.entries[idx] = clockEntry[V]{key: 0, val: zero, ref: false}
}
