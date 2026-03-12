package tombstone

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func TestCompactionTrackerCoversWithMonotonicKeys(t *testing.T) {
	tracker := NewCompactionTracker()

	// Compaction iterates keys in ascending order. Once a range tombstone
	// [b,f)@10 has been seen, subsequent keys >= b should respect it until f.
	tracker.Add(Range{
		CF:      kv.CFDefault,
		Start:   []byte("b"),
		End:     []byte("f"),
		Version: 10,
	})

	if !tracker.Covers(kv.CFDefault, []byte("b"), 1) {
		t.Fatalf("expected key b@1 to be covered")
	}
	if !tracker.Covers(kv.CFDefault, []byte("e"), 9) {
		t.Fatalf("expected key e@9 to be covered")
	}
	if tracker.Covers(kv.CFDefault, []byte("e"), 10) {
		t.Fatalf("expected key e@10 not covered when versions are equal")
	}
	if tracker.Covers(kv.CFDefault, []byte("f"), 1) {
		t.Fatalf("expected key f@1 not covered (end is exclusive)")
	}
}

func TestCompactionTrackerOverlapAndCFIsolation(t *testing.T) {
	tracker := NewCompactionTracker()

	tracker.Add(Range{
		CF:      kv.CFDefault,
		Start:   []byte("a"),
		End:     []byte("z"),
		Version: 100,
	})
	tracker.Add(Range{
		CF:      kv.CFDefault,
		Start:   []byte("d"),
		End:     []byte("h"),
		Version: 200,
	})

	if !tracker.Covers(kv.CFDefault, []byte("e"), 150) {
		t.Fatalf("expected key e@150 covered by newer overlapping tombstone")
	}
	if tracker.Covers(kv.CFDefault, []byte("e"), 200) {
		t.Fatalf("expected key e@200 not covered when versions are equal")
	}
	if !tracker.Covers(kv.CFDefault, []byte("y"), 99) {
		t.Fatalf("expected key y@99 covered by wide tombstone")
	}
	if tracker.Covers(kv.CFLock, []byte("e"), 1) {
		t.Fatalf("expected key e@1 in lock CF not covered by default CF tombstones")
	}
}
