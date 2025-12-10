package lsm

import "sync"

// clockProCache is a simple CLOCK-style cache for a small hot set keyed by uint64.
// It keeps a single hand and a ref bit; on miss it scans until it finds an
// unreferenced entry to evict. This is intentionally tiny and lock-protected
// because hot sets are small.
type clockProCache[V any] struct {
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

func newClockProCache[V any](capacity int) *clockProCache[V] {
	if capacity <= 0 {
		return nil
	}
	return &clockProCache[V]{
		cap:   capacity,
		index: make(map[uint64]int, capacity),
	}
}

func (c *clockProCache[V]) get(key uint64) (V, bool) {
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

func (c *clockProCache[V]) promote(key uint64, val V) {
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
	// Evict using CLOCK hand.
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
