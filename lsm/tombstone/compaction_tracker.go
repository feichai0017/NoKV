package tombstone

import (
	"bytes"
	"container/heap"

	"github.com/feichai0017/NoKV/kv"
)

type rangeEnd struct {
	end     []byte
	version uint64
}

type rangeEndHeap []rangeEnd

func (h rangeEndHeap) Len() int           { return len(h) }
func (h rangeEndHeap) Less(i, j int) bool { return bytes.Compare(h[i].end, h[j].end) < 0 }
func (h rangeEndHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rangeEndHeap) Push(x any)        { *h = append(*h, x.(rangeEnd)) }
func (h *rangeEndHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

type cfTracker struct {
	ends     rangeEndHeap
	versions *versionSet
}

func newCFTracker() *cfTracker {
	return &cfTracker{
		versions: newVersionSet(16),
	}
}

func (t *cfTracker) add(end []byte, version uint64) {
	heap.Push(&t.ends, rangeEnd{
		end:     kv.SafeCopy(nil, end),
		version: version,
	})
	t.versions.add(version)
}

func (t *cfTracker) evict(userKey []byte) {
	for t.ends.Len() > 0 {
		head := t.ends[0]
		if bytes.Compare(head.end, userKey) > 0 {
			break
		}
		popped := heap.Pop(&t.ends).(rangeEnd)
		t.versions.remove(popped.version)
	}
}

func (t *cfTracker) covers(userKey []byte, version uint64) bool {
	t.evict(userKey)
	maxVersion, ok := t.versions.max()
	if !ok {
		return false
	}
	return maxVersion > version
}

// CompactionTracker tracks active range tombstones while scanning keys in
// monotonic order (typical compaction merge loop).
type CompactionTracker struct {
	byCF map[kv.ColumnFamily]*cfTracker
}

// NewCompactionTracker creates an empty tracker.
func NewCompactionTracker() *CompactionTracker {
	return &CompactionTracker{
		byCF: make(map[kv.ColumnFamily]*cfTracker, 3),
	}
}

// Add registers a range tombstone in the active tracker.
func (t *CompactionTracker) Add(rt Range) {
	if t == nil {
		return
	}
	normalized, ok := normalizeRange(rt)
	if !ok {
		return
	}
	tracker := t.byCF[normalized.CF]
	if tracker == nil {
		tracker = newCFTracker()
		t.byCF[normalized.CF] = tracker
	}
	tracker.add(normalized.End, normalized.Version)
}

// Covers reports whether key@version is covered by currently active tombstones
// for the given CF.
func (t *CompactionTracker) Covers(cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	if t == nil {
		return false
	}
	tracker := t.byCF[cf]
	if tracker == nil {
		return false
	}
	return tracker.covers(userKey, version)
}
