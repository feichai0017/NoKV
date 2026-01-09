package utils

import (
	"math"
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func TestARTGetLatest(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	versions := []uint64{3, 1, 2}
	values := [][]byte{[]byte("v3"), []byte("v1"), []byte("v2")}
	for i, ver := range versions {
		entry := kv.NewEntryWithCF(kv.CFDefault, []byte("k"), values[i])
		entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, ver)
		art.Add(entry)
		entry.DecrRef()
	}

	seekKey := kv.InternalKey(kv.CFDefault, []byte("k"), math.MaxUint32)
	vs := art.Search(seekKey)
	if string(vs.Value) != "v3" {
		t.Fatalf("expected latest value v3, got %q", string(vs.Value))
	}
}

func TestARTIteratorOrder(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	keys := [][]byte{[]byte("b"), []byte("a"), []byte("c"), []byte("a")}
	vers := []uint64{2, 3, 1, 1}
	for i, k := range keys {
		entry := kv.NewEntryWithCF(kv.CFDefault, k, []byte("v"))
		entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, vers[i])
		art.Add(entry)
		entry.DecrRef()
	}

	it := art.NewIterator(nil)
	if it == nil {
		t.Fatalf("expected iterator")
	}
	defer it.Close()

	it.Rewind()
	var last []byte
	for ; it.Valid(); it.Next() {
		entry := it.Item().Entry()
		if entry == nil {
			t.Fatalf("nil entry")
		}
		if last != nil && CompareKeys(last, entry.Key) > 0 {
			t.Fatalf("iterator out of order: %q before %q", last, entry.Key)
		}
		last = entry.Key
	}

	seek := kv.InternalKey(kv.CFDefault, []byte("b"), math.MaxUint32)
	it.Seek(seek)
	if !it.Valid() {
		t.Fatalf("expected seek to be valid")
	}
	entry := it.Item().Entry()
	if entry == nil || !kv.SameKey(seek, entry.Key) {
		t.Fatalf("seek mismatch: got %v", entry)
	}
}
