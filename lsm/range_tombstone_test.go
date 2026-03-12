package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/tombstone"
)

func TestRangeTombstoneCollector_Add(t *testing.T) {
	c := tombstone.NewCollector()
	if c.Count() != 0 {
		t.Fatal("new collector should be empty")
	}

	c.Add(tombstone.Range{
		CF:      kv.CFDefault,
		Start:   []byte("a"),
		End:     []byte("z"),
		Version: 100,
	})

	if c.Count() != 1 {
		t.Errorf("expected count 1, got %d", c.Count())
	}
}

func TestRangeTombstoneCollector_IsKeyCovered(t *testing.T) {
	c := tombstone.NewCollector()
	c.Add(tombstone.Range{
		CF:      kv.CFDefault,
		Start:   []byte("b"),
		End:     []byte("d"),
		Version: 100,
	})

	tests := []struct {
		cf      kv.ColumnFamily
		key     []byte
		version uint64
		covered bool
	}{
		{kv.CFDefault, []byte("a"), 50, false},  // before range
		{kv.CFDefault, []byte("b"), 50, true},   // start (inclusive)
		{kv.CFDefault, []byte("c"), 50, true},   // middle
		{kv.CFDefault, []byte("d"), 50, false},  // end (exclusive)
		{kv.CFDefault, []byte("e"), 50, false},  // after range
		{kv.CFDefault, []byte("b"), 100, false}, // version equal (not covered)
		{kv.CFDefault, []byte("b"), 101, false}, // version newer
		{kv.CFLock, []byte("b"), 50, false},     // different CF
	}

	for _, tt := range tests {
		got := c.IsKeyCovered(tt.cf, tt.key, tt.version)
		if got != tt.covered {
			t.Errorf("IsKeyCovered(%v, %s, %d) = %v, want %v",
				tt.cf, tt.key, tt.version, got, tt.covered)
		}
	}
}

func TestRangeTombstoneCollector_Rebuild(t *testing.T) {
	c := tombstone.NewCollector()
	c.Add(tombstone.Range{CF: kv.CFDefault, Start: []byte("a"), End: []byte("b"), Version: 1})
	c.Add(tombstone.Range{CF: kv.CFDefault, Start: []byte("c"), End: []byte("d"), Version: 2})

	if c.Count() != 2 {
		t.Fatal("expected 2 tombstones")
	}

	c.Rebuild([]tombstone.Range{
		{CF: kv.CFDefault, Start: []byte("x"), End: []byte("z"), Version: 10},
	})

	if c.Count() != 1 {
		t.Errorf("expected count 1 after rebuild, got %d", c.Count())
	}

	if !c.IsKeyCovered(kv.CFDefault, []byte("y"), 5) {
		t.Error("key y should be covered after rebuild")
	}
	if c.IsKeyCovered(kv.CFDefault, []byte("a"), 1) {
		t.Error("key a should not be covered after rebuild")
	}
}

func TestRangeTombstoneCollector_Overlapping(t *testing.T) {
	c := tombstone.NewCollector()
	c.Add(tombstone.Range{CF: kv.CFDefault, Start: []byte("a"), End: []byte("m"), Version: 100})
	c.Add(tombstone.Range{CF: kv.CFDefault, Start: []byte("h"), End: []byte("z"), Version: 200})

	if !c.IsKeyCovered(kv.CFDefault, []byte("j"), 50) {
		t.Error("key j should be covered by first tombstone")
	}
	if !c.IsKeyCovered(kv.CFDefault, []byte("j"), 150) {
		t.Error("key j should be covered by second tombstone")
	}
}

func TestRangeTombstoneCollector_CFIsolation(t *testing.T) {
	c := tombstone.NewCollector()
	c.Add(tombstone.Range{CF: kv.CFDefault, Start: []byte("a"), End: []byte("z"), Version: 100})
	c.Add(tombstone.Range{CF: kv.CFLock, Start: []byte("a"), End: []byte("z"), Version: 100})

	if !c.IsKeyCovered(kv.CFDefault, []byte("m"), 50) {
		t.Error("key should be covered in CFDefault")
	}
	if !c.IsKeyCovered(kv.CFLock, []byte("m"), 50) {
		t.Error("key should be covered in CFLock")
	}
	if c.IsKeyCovered(kv.CFWrite, []byte("m"), 50) {
		t.Error("key should not be covered in CFWrite")
	}
}
