package index

import (
	"encoding/binary"
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func makeSkiplistKey(i int) []byte {
	key := make([]byte, 16)
	copy(key, "benchkey")
	binary.LittleEndian.PutUint64(key[8:], uint64(i))
	return kv.InternalKey(kv.CFDefault, key, uint64(i+1))
}

func BenchmarkSkiplistInsert(b *testing.B) {
	list := NewSkiplist(1 << 20)
	value := make([]byte, 64)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := kv.NewEntry(makeSkiplistKey(i), value)
		list.Add(entry)
		entry.DecrRef()
	}
}

func BenchmarkSkiplistGet(b *testing.B) {
	list := NewSkiplist(1 << 20)
	value := make([]byte, 64)
	keys := make([][]byte, 10_000)
	for i := range keys {
		key := makeSkiplistKey(i)
		entry := kv.NewEntry(key, value)
		list.Add(entry)
		entry.DecrRef()
		keys[i] = key
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, vs := list.Search(keys[i%len(keys)])
		if len(vs.Value) == 0 {
			b.Fatalf("missing value")
		}
	}
}

func BenchmarkSkiplistSeek(b *testing.B) {
	list := NewSkiplist(1 << 20)
	value := make([]byte, 64)
	keys := make([][]byte, 10_000)
	for i := range keys {
		key := makeSkiplistKey(i)
		entry := kv.NewEntry(key, value)
		list.Add(entry)
		entry.DecrRef()
		keys[i] = key
	}

	it := list.NewIterator(nil)
	defer func() { _ = it.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it.Seek(keys[i%len(keys)])
		if !it.Valid() {
			b.Fatalf("seek missed key")
		}
	}
}

func BenchmarkSkiplistIteratorNext(b *testing.B) {
	list := NewSkiplist(1 << 20)
	value := make([]byte, 64)
	for i := range 10_000 {
		entry := kv.NewEntry(makeSkiplistKey(i), value)
		list.Add(entry)
		entry.DecrRef()
	}

	it := list.NewIterator(nil)
	defer func() { _ = it.Close() }()
	it.Rewind()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.Rewind()
		}
		if !it.Valid() {
			b.Fatalf("iterator unexpectedly invalid after rewind")
		}
		_ = it.Item()
		it.Next()
	}
}

func makeSequentialSkiplistKey(i int) []byte {
	key := make([]byte, 16)
	copy(key, "benchkey")
	binary.BigEndian.PutUint64(key[8:], uint64(i))
	return kv.InternalKey(kv.CFDefault, key, uint64(i+1))
}

// makeRandomSkiplistKey creates a random key for testing non-sequential inserts
func makeRandomSkiplistKey(i int) []byte {
	// Use a pseudo-random permutation of i to simulate random inserts
	// This ensures the same sequence of keys for fair comparison
	permuted := uint64((i*0x5deece66d + 0xb) & 0xffffffffffff)
	key := make([]byte, 16)
	copy(key, "benchkey")
	binary.BigEndian.PutUint64(key[8:], permuted)
	return kv.InternalKey(kv.CFDefault, key, uint64(i+1))
}

// BenchmarkSkiplistInsertSequential tests sequential insert performance (uses fast path)
func BenchmarkSkiplistInsertSequential(b *testing.B) {
	list := NewSkiplist(1 << 20)
	value := make([]byte, 64)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := kv.NewEntry(makeSequentialSkiplistKey(i), value)
		list.Add(entry)
		entry.DecrRef()
	}
}

// BenchmarkSkiplistInsertRandom tests random insert performance (uses normal path)
func BenchmarkSkiplistInsertRandom(b *testing.B) {
	list := NewSkiplist(1 << 20)
	value := make([]byte, 64)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := kv.NewEntry(makeRandomSkiplistKey(i), value)
		list.Add(entry)
		entry.DecrRef()
	}
}
