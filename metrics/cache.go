package metrics

import "sync/atomic"

// CacheSnapshot captures cache hit/miss counters for read path observability.
type CacheSnapshot struct {
	L0Hits      uint64
	L0Misses    uint64
	L1Hits      uint64
	L1Misses    uint64
	IndexHits   uint64
	IndexMisses uint64
}

// CacheCounters records cache hits/misses for blocks, blooms, and indexes.
type CacheCounters struct {
	l0Hits      atomic.Uint64
	l0Misses    atomic.Uint64
	l1Hits      atomic.Uint64
	l1Misses    atomic.Uint64
	indexHits   atomic.Uint64
	indexMisses atomic.Uint64
}

func NewCacheCounters() *CacheCounters {
	return &CacheCounters{}
}

func (m *CacheCounters) RecordBlock(level int, hit bool) {
	switch level {
	case 0:
		if hit {
			m.l0Hits.Add(1)
		} else {
			m.l0Misses.Add(1)
		}
	case 1:
		if hit {
			m.l1Hits.Add(1)
		} else {
			m.l1Misses.Add(1)
		}
	}
}

func (m *CacheCounters) RecordIndex(hit bool) {
	if hit {
		m.indexHits.Add(1)
		return
	}
	m.indexMisses.Add(1)
}

func (m *CacheCounters) Snapshot() CacheSnapshot {
	if m == nil {
		return CacheSnapshot{}
	}
	return CacheSnapshot{
		L0Hits:      m.l0Hits.Load(),
		L0Misses:    m.l0Misses.Load(),
		L1Hits:      m.l1Hits.Load(),
		L1Misses:    m.l1Misses.Load(),
		IndexHits:   m.indexHits.Load(),
		IndexMisses: m.indexMisses.Load(),
	}
}
