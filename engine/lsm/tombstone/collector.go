package tombstone

import (
	"bytes"
	"container/heap"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/engine/kv"
)

// Range represents a single range deletion marker held in memory.
type Range struct {
	CF      kv.ColumnFamily
	Start   []byte // inclusive
	End     []byte // exclusive
	Version uint64
}

// Span is a compact non-overlapping segment annotated with the max covering
// tombstone version.
type Span struct {
	Start      []byte
	End        []byte
	MaxVersion uint64
}

type bucket struct {
	tombstones []Range
	spans      []Span
}

// Collector maintains an in-memory index of active range tombstones for
// read-path coverage checks. Tombstones are bucketed by CF and fragmented into
// non-overlapping spans so point checks are O(log M).
type Collector struct {
	mu      sync.RWMutex
	buckets map[kv.ColumnFamily]*bucket
	count   int
	dirty   bool
}

// NewCollector creates a new empty collector.
func NewCollector() *Collector {
	return &Collector{
		buckets: make(map[kv.ColumnFamily]*bucket),
	}
}

// IsKeyCovered checks if userKey@version in the given CF is covered by any
// tombstone. A tombstone covers the key when:
//   - CF matches
//   - tombstone version > key version
//   - userKey is in [Start, End)
func (c *Collector) IsKeyCovered(cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	dirty := c.dirty
	var spans []Span
	if !dirty {
		if b := c.buckets[cf]; b != nil {
			spans = b.spans
		}
	}
	c.mu.RUnlock()
	if !dirty {
		return IsKeyCoveredBySpans(spans, userKey, version)
	}

	c.mu.Lock()
	if c.dirty {
		rebuildCollectorSpans(c.buckets)
		c.dirty = false
	}
	if b := c.buckets[cf]; b != nil {
		spans = b.spans
	}
	c.mu.Unlock()
	return IsKeyCoveredBySpans(spans, userKey, version)
}

// Add appends a single tombstone to the collector and marks the span index
// dirty.
func (c *Collector) Add(rt Range) {
	normalized, ok := normalizeRange(rt)
	if !ok {
		return
	}
	c.mu.Lock()
	b := c.buckets[normalized.CF]
	if b == nil {
		b = &bucket{}
		c.buckets[normalized.CF] = b
	}
	b.tombstones = append(b.tombstones, normalized)
	c.count++
	c.dirty = true
	c.mu.Unlock()
}

// Rebuild completely replaces the tombstone set.
func (c *Collector) Rebuild(tombstones []Range) {
	buckets, count := buildCollectorBuckets(tombstones)
	c.mu.Lock()
	c.buckets = buckets
	c.count = count
	c.dirty = false
	c.mu.Unlock()
}

// Count returns the current number of tracked tombstones.
func (c *Collector) Count() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}

func normalizeRange(rt Range) (Range, bool) {
	if bytes.Compare(rt.Start, rt.End) >= 0 {
		return Range{}, false
	}
	if !rt.CF.Valid() {
		rt.CF = kv.CFDefault
	}
	return Range{
		CF:      rt.CF,
		Start:   kv.SafeCopy(nil, rt.Start),
		End:     kv.SafeCopy(nil, rt.End),
		Version: rt.Version,
	}, true
}

func buildCollectorBuckets(tombstones []Range) (map[kv.ColumnFamily]*bucket, int) {
	buckets := make(map[kv.ColumnFamily]*bucket, 3)
	count := 0
	for i := range tombstones {
		normalized, ok := normalizeRange(tombstones[i])
		if !ok {
			continue
		}
		b := buckets[normalized.CF]
		if b == nil {
			b = &bucket{}
			buckets[normalized.CF] = b
		}
		b.tombstones = append(b.tombstones, normalized)
		count++
	}
	rebuildCollectorSpans(buckets)
	return buckets, count
}

// BuildCFSpans groups ranges by CF and builds non-overlapping spans for each CF.
func BuildCFSpans(tombstones []Range) map[kv.ColumnFamily][]Span {
	if len(tombstones) == 0 {
		return nil
	}
	perCF := make(map[kv.ColumnFamily][]Range, 3)
	for i := range tombstones {
		normalized, ok := normalizeRange(tombstones[i])
		if !ok {
			continue
		}
		perCF[normalized.CF] = append(perCF[normalized.CF], normalized)
	}
	if len(perCF) == 0 {
		return nil
	}
	index := make(map[kv.ColumnFamily][]Span, len(perCF))
	for cf, ranges := range perCF {
		index[cf] = BuildSpans(ranges)
	}
	return index
}

func rebuildCollectorSpans(buckets map[kv.ColumnFamily]*bucket) {
	for _, b := range buckets {
		b.spans = BuildSpans(b.tombstones)
	}
}

type boundaryEvents struct {
	starts []uint64
	ends   []uint64
}

// BuildSpans builds non-overlapping spans from range tombstones.
func BuildSpans(tombstones []Range) []Span {
	if len(tombstones) == 0 {
		return nil
	}
	events := make(map[string]*boundaryEvents, len(tombstones)*2)
	boundaries := make([][]byte, 0, len(tombstones)*2)
	addBoundary := func(key []byte, version uint64, isStart bool) {
		token := string(key)
		ev := events[token]
		if ev == nil {
			ev = &boundaryEvents{}
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
		normalized, ok := normalizeRange(tombstones[i])
		if !ok {
			continue
		}
		addBoundary(normalized.Start, normalized.Version, true)
		addBoundary(normalized.End, normalized.Version, false)
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

	active := newVersionSet(len(tombstones))
	spans := make([]Span, 0, len(uniq)-1)
	for i := 0; i < len(uniq)-1; i++ {
		key := uniq[i]
		if ev := events[string(key)]; ev != nil {
			for _, v := range ev.ends {
				active.remove(v)
			}
			for _, v := range ev.starts {
				active.add(v)
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
			spans[n-1].MaxVersion == maxVersion &&
			bytes.Equal(spans[n-1].End, key) {
			spans[n-1].End = kv.SafeCopy(spans[n-1].End, next)
			continue
		}
		spans = append(spans, Span{
			Start:      kv.SafeCopy(nil, key),
			End:        kv.SafeCopy(nil, next),
			MaxVersion: maxVersion,
		})
	}
	return spans
}

// IsKeyCoveredBySpans checks whether key@version is covered by prebuilt spans.
func IsKeyCoveredBySpans(spans []Span, userKey []byte, version uint64) bool {
	if len(spans) == 0 {
		return false
	}
	idx := sort.Search(len(spans), func(i int) bool {
		return bytes.Compare(spans[i].End, userKey) > 0
	})
	if idx >= len(spans) {
		return false
	}
	span := spans[idx]
	if bytes.Compare(userKey, span.Start) < 0 || bytes.Compare(userKey, span.End) >= 0 {
		return false
	}
	return span.MaxVersion > version
}

type versionSet struct {
	counts map[uint64]int
	heap   maxVersionHeap
}

func newVersionSet(capacity int) *versionSet {
	return &versionSet{
		counts: make(map[uint64]int, capacity),
	}
}

func (s *versionSet) add(version uint64) {
	if s.counts[version] == 0 {
		heap.Push(&s.heap, version)
	}
	s.counts[version]++
}

func (s *versionSet) remove(version uint64) {
	count := s.counts[version]
	if count <= 1 {
		delete(s.counts, version)
		return
	}
	s.counts[version] = count - 1
}

func (s *versionSet) max() (uint64, bool) {
	for s.heap.Len() > 0 {
		top := s.heap[0]
		if s.counts[top] > 0 {
			return top, true
		}
		_ = heap.Pop(&s.heap)
	}
	return 0, false
}

type maxVersionHeap []uint64

func (h maxVersionHeap) Len() int           { return len(h) }
func (h maxVersionHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h maxVersionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxVersionHeap) Push(x any)        { *h = append(*h, x.(uint64)) }
func (h *maxVersionHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}
