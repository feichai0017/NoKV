package lsm

import (
	"bytes"
	"container/heap"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

// RangeTombstone represents a single range deletion marker held in memory.
type RangeTombstone struct {
	CF      kv.ColumnFamily
	Start   []byte // inclusive
	End     []byte // exclusive
	Version uint64
}

type rangeTombstoneSpan struct {
	start      []byte
	end        []byte
	maxVersion uint64
}

type rangeTombstoneBucket struct {
	tombstones []RangeTombstone
	spans      []rangeTombstoneSpan
}

// RangeTombstoneCollector maintains an in-memory index of active range
// tombstones for read-path coverage checks. Tombstones are bucketed by CF and
// fragmented into non-overlapping spans so point checks are O(log M).
type RangeTombstoneCollector struct {
	mu      sync.RWMutex
	buckets map[kv.ColumnFamily]*rangeTombstoneBucket
	count   int
	dirty   bool
}

// NewRangeTombstoneCollector creates a new empty collector.
func NewRangeTombstoneCollector() *RangeTombstoneCollector {
	return &RangeTombstoneCollector{
		buckets: make(map[kv.ColumnFamily]*rangeTombstoneBucket),
	}
}

// IsKeyCovered checks if userKey@version in the given CF is covered
// by any range tombstone. A tombstone covers the key when:
//   - CF matches
//   - tombstone version > key version
//   - userKey is in [Start, End)
func (c *RangeTombstoneCollector) IsKeyCovered(cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	dirty := c.dirty
	var spans []rangeTombstoneSpan
	if !dirty {
		if bucket := c.buckets[cf]; bucket != nil {
			spans = bucket.spans
		}
	}
	c.mu.RUnlock()
	if !dirty {
		return isKeyCoveredBySpans(spans, userKey, version)
	}
	c.mu.Lock()
	if c.dirty {
		rebuildCollectorSpans(c.buckets)
		c.dirty = false
	}
	if bucket := c.buckets[cf]; bucket != nil {
		spans = bucket.spans
	} else {
		spans = nil
	}
	c.mu.Unlock()
	return isKeyCoveredBySpans(spans, userKey, version)
}

// Add appends a single tombstone to the collector and marks the span index dirty.
func (c *RangeTombstoneCollector) Add(rt RangeTombstone) {
	normalized, ok := normalizeRangeTombstone(rt)
	if !ok {
		return
	}
	c.mu.Lock()
	bucket := c.buckets[normalized.CF]
	if bucket == nil {
		bucket = &rangeTombstoneBucket{}
		c.buckets[normalized.CF] = bucket
	}
	bucket.tombstones = append(bucket.tombstones, normalized)
	c.count++
	c.dirty = true
	c.mu.Unlock()
}

// Rebuild completely replaces the tombstone set. Tombstones are sorted
// and fragmented by CF for O(log M) coverage checks.
func (c *RangeTombstoneCollector) Rebuild(tombstones []RangeTombstone) {
	buckets, count := buildCollectorBuckets(tombstones)
	c.mu.Lock()
	c.buckets = buckets
	c.count = count
	c.dirty = false
	c.mu.Unlock()
}

// Count returns the current number of tracked tombstones.
func (c *RangeTombstoneCollector) Count() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}

func normalizeRangeTombstone(rt RangeTombstone) (RangeTombstone, bool) {
	if bytes.Compare(rt.Start, rt.End) >= 0 {
		return RangeTombstone{}, false
	}
	if !rt.CF.Valid() {
		rt.CF = kv.CFDefault
	}
	return RangeTombstone{
		CF:      rt.CF,
		Start:   kv.SafeCopy(nil, rt.Start),
		End:     kv.SafeCopy(nil, rt.End),
		Version: rt.Version,
	}, true
}

func buildCollectorBuckets(tombstones []RangeTombstone) (map[kv.ColumnFamily]*rangeTombstoneBucket, int) {
	buckets := make(map[kv.ColumnFamily]*rangeTombstoneBucket, 3)
	count := 0
	for _, rt := range tombstones {
		normalized, ok := normalizeRangeTombstone(rt)
		if !ok {
			continue
		}
		bucket := buckets[normalized.CF]
		if bucket == nil {
			bucket = &rangeTombstoneBucket{}
			buckets[normalized.CF] = bucket
		}
		bucket.tombstones = append(bucket.tombstones, normalized)
		count++
	}
	rebuildCollectorSpans(buckets)
	return buckets, count
}

func rebuildCollectorSpans(buckets map[kv.ColumnFamily]*rangeTombstoneBucket) {
	for _, bucket := range buckets {
		bucket.spans = buildRangeSpans(bucket.tombstones)
	}
}

func buildMemRangeIndex(tombstones []memRangeTombstone) map[kv.ColumnFamily][]rangeTombstoneSpan {
	if len(tombstones) == 0 {
		return nil
	}
	perCF := make(map[kv.ColumnFamily][]RangeTombstone, 3)
	for _, rt := range tombstones {
		normalized, ok := normalizeRangeTombstone(RangeTombstone{
			CF:      rt.cf,
			Start:   rt.start,
			End:     rt.end,
			Version: rt.version,
		})
		if !ok {
			continue
		}
		perCF[normalized.CF] = append(perCF[normalized.CF], normalized)
	}
	if len(perCF) == 0 {
		return nil
	}
	index := make(map[kv.ColumnFamily][]rangeTombstoneSpan, len(perCF))
	for cf, ranges := range perCF {
		index[cf] = buildRangeSpans(ranges)
	}
	return index
}

type rangeBoundaryEvents struct {
	starts []uint64
	ends   []uint64
}

func buildRangeSpans(tombstones []RangeTombstone) []rangeTombstoneSpan {
	if len(tombstones) == 0 {
		return nil
	}
	events := make(map[string]*rangeBoundaryEvents, len(tombstones)*2)
	boundaries := make([][]byte, 0, len(tombstones)*2)
	addBoundary := func(key []byte, version uint64, isStart bool) {
		token := string(key)
		ev := events[token]
		if ev == nil {
			ev = &rangeBoundaryEvents{}
			events[token] = ev
			boundaries = append(boundaries, kv.SafeCopy(nil, key))
		}
		if isStart {
			ev.starts = append(ev.starts, version)
			return
		}
		ev.ends = append(ev.ends, version)
	}
	for i := range tombstones {
		rt := &tombstones[i]
		if bytes.Compare(rt.Start, rt.End) >= 0 {
			continue
		}
		addBoundary(rt.Start, rt.Version, true)
		addBoundary(rt.End, rt.Version, false)
	}
	if len(boundaries) < 2 {
		return nil
	}
	sort.Slice(boundaries, func(i, j int) bool {
		return bytes.Compare(boundaries[i], boundaries[j]) < 0
	})
	uniq := boundaries[:0]
	for i := range boundaries {
		if len(uniq) == 0 || !bytes.Equal(uniq[len(uniq)-1], boundaries[i]) {
			uniq = append(uniq, boundaries[i])
		}
	}
	if len(uniq) < 2 {
		return nil
	}
	active := newRangeVersionSet(len(tombstones))
	spans := make([]rangeTombstoneSpan, 0, len(uniq)-1)
	for i := 0; i < len(uniq)-1; i++ {
		key := uniq[i]
		if ev := events[string(key)]; ev != nil {
			for _, version := range ev.ends {
				active.remove(version)
			}
			for _, version := range ev.starts {
				active.add(version)
			}
		}
		next := uniq[i+1]
		if bytes.Compare(key, next) >= 0 {
			continue
		}
		maxVersion, ok := active.max()
		if !ok {
			continue
		}
		if n := len(spans); n > 0 &&
			spans[n-1].maxVersion == maxVersion &&
			bytes.Equal(spans[n-1].end, key) {
			spans[n-1].end = kv.SafeCopy(spans[n-1].end, next)
			continue
		}
		spans = append(spans, rangeTombstoneSpan{
			start:      kv.SafeCopy(nil, key),
			end:        kv.SafeCopy(nil, next),
			maxVersion: maxVersion,
		})
	}
	return spans
}

func isKeyCoveredBySpans(spans []rangeTombstoneSpan, userKey []byte, version uint64) bool {
	if len(spans) == 0 {
		return false
	}
	idx := sort.Search(len(spans), func(i int) bool {
		return bytes.Compare(spans[i].end, userKey) > 0
	})
	if idx >= len(spans) {
		return false
	}
	span := spans[idx]
	if bytes.Compare(userKey, span.start) < 0 || bytes.Compare(userKey, span.end) >= 0 {
		return false
	}
	return span.maxVersion > version
}

type rangeVersionSet struct {
	counts map[uint64]int
	heap   rangeMaxVersionHeap
}

func newRangeVersionSet(capacity int) *rangeVersionSet {
	return &rangeVersionSet{
		counts: make(map[uint64]int, capacity),
	}
}

func (s *rangeVersionSet) add(version uint64) {
	if s.counts[version] == 0 {
		heap.Push(&s.heap, version)
	}
	s.counts[version]++
}

func (s *rangeVersionSet) remove(version uint64) {
	if s == nil {
		return
	}
	count := s.counts[version]
	if count <= 1 {
		delete(s.counts, version)
		return
	}
	s.counts[version] = count - 1
}

func (s *rangeVersionSet) max() (uint64, bool) {
	if s == nil {
		return 0, false
	}
	for s.heap.Len() > 0 {
		top := s.heap[0]
		if s.counts[top] > 0 {
			return top, true
		}
		_ = heap.Pop(&s.heap)
	}
	return 0, false
}

type rangeMaxVersionHeap []uint64

func (h rangeMaxVersionHeap) Len() int {
	return len(h)
}

func (h rangeMaxVersionHeap) Less(i, j int) bool {
	return h[i] > h[j]
}

func (h rangeMaxVersionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *rangeMaxVersionHeap) Push(x any) {
	*h = append(*h, x.(uint64))
}

func (h *rangeMaxVersionHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// rebuildRangeTombstones scans SST levels to repopulate the range tombstone
// collector. Memtable tombstones are tracked separately in memTable.rangeTombstones
// and must not be included here to avoid duplication when those memtables flush.
// This is called at startup and after max-level compaction (which may drop tombstones).
func (lm *levelManager) rebuildRangeTombstones() {
	if lm == nil || lm.rtCollector == nil || len(lm.levels) == 0 {
		return
	}
	var tombstones []RangeTombstone
	opt := &utils.Options{IsAsc: true}
	// Only scan SST levels — memtable tombstones are tracked separately
	// in memTable.rangeTombstones and must not be duplicated here.
	iters := lm.iterators(opt)
	defer func() {
		for _, it := range iters {
			if it != nil {
				_ = it.Close()
			}
		}
	}()
	for _, it := range iters {
		if it == nil {
			continue
		}
		it.Rewind()
		for it.Valid() {
			if item := it.Item(); item != nil {
				if e := item.Entry(); e != nil && e.IsRangeDelete() {
					cf, start, version, ok := kv.SplitInternalKey(e.Key)
					if !ok {
						it.Next()
						continue
					}
					tombstones = append(tombstones, RangeTombstone{
						CF:      cf,
						Start:   kv.SafeCopy(nil, start),
						End:     kv.SafeCopy(nil, e.RangeEnd()),
						Version: version,
					})
				}
			}
			it.Next()
		}
	}
	lm.rtCollector.Rebuild(tombstones)
}
