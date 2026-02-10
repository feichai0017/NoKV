package metrics

import "sync/atomic"

// CacheSnapshot captures cache hit/miss counters for read path observability.
type CacheSnapshot struct {
	L0Hits      uint64
	L0Misses    uint64
	L1Hits      uint64
	L1Misses    uint64
	BloomHits   uint64
	BloomMisses uint64
	IndexHits   uint64
	IndexMisses uint64
}

// CacheCounters records cache hits/misses for blocks, blooms, and indexes.
type CacheCounters struct {
	l0Hits      uint64
	l0Misses    uint64
	l1Hits      uint64
	l1Misses    uint64
	bloomHits   uint64
	bloomMisses uint64
	indexHits   uint64
	indexMisses uint64
}

// NewCacheCounters creates a new value for the API.
func NewCacheCounters() *CacheCounters {
	return &CacheCounters{}
}

// RecordBlock is part of the exported receiver API.
func (m *CacheCounters) RecordBlock(level int, hit bool) {
	switch level {
	case 0:
		if hit {
			atomic.AddUint64(&m.l0Hits, 1)
		} else {
			atomic.AddUint64(&m.l0Misses, 1)
		}
	case 1:
		if hit {
			atomic.AddUint64(&m.l1Hits, 1)
		} else {
			atomic.AddUint64(&m.l1Misses, 1)
		}
	}
}

// RecordBloom is part of the exported receiver API.
func (m *CacheCounters) RecordBloom(hit bool) {
	if hit {
		atomic.AddUint64(&m.bloomHits, 1)
		return
	}
	atomic.AddUint64(&m.bloomMisses, 1)
}

// RecordIndex is part of the exported receiver API.
func (m *CacheCounters) RecordIndex(hit bool) {
	if hit {
		atomic.AddUint64(&m.indexHits, 1)
		return
	}
	atomic.AddUint64(&m.indexMisses, 1)
}

// Snapshot is part of the exported receiver API.
func (m *CacheCounters) Snapshot() CacheSnapshot {
	if m == nil {
		return CacheSnapshot{}
	}
	return CacheSnapshot{
		L0Hits:      atomic.LoadUint64(&m.l0Hits),
		L0Misses:    atomic.LoadUint64(&m.l0Misses),
		L1Hits:      atomic.LoadUint64(&m.l1Hits),
		L1Misses:    atomic.LoadUint64(&m.l1Misses),
		BloomHits:   atomic.LoadUint64(&m.bloomHits),
		BloomMisses: atomic.LoadUint64(&m.bloomMisses),
		IndexHits:   atomic.LoadUint64(&m.indexHits),
		IndexMisses: atomic.LoadUint64(&m.indexMisses),
	}
}
