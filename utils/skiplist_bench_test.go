package utils

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
		vs := list.Search(keys[i%len(keys)])
		if len(vs.Value) == 0 {
			b.Fatalf("missing value")
		}
	}
}
