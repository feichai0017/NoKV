package utils

import (
	"encoding/binary"
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func makeARTKey(i int) []byte {
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, uint32(i))
	return kv.InternalKey(kv.CFDefault, key, uint64(i+1))
}

func BenchmarkARTInsert(b *testing.B) {
	const (
		arenaSize = 1 << 20
		keySpace  = 4096
	)
	art := NewART(arenaSize)
	defer func() {
		if art != nil {
			art.DecrRef()
		}
	}()
	value := make([]byte, 64)
	keys := make([][]byte, keySpace)
	for i := range keys {
		keys[i] = makeARTKey(i)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%keySpace == 0 {
			old := art
			art = NewART(arenaSize)
			old.DecrRef()
		}
		entry := kv.NewEntry(keys[i%keySpace], value)
		art.Add(entry)
		entry.DecrRef()
	}
}

func BenchmarkARTGet(b *testing.B) {
	const (
		arenaSize = 1 << 20
		keySpace  = 4096
	)
	art := NewART(arenaSize)
	defer art.DecrRef()
	value := make([]byte, 64)
	keys := make([][]byte, keySpace)
	for i := range keys {
		key := makeARTKey(i)
		entry := kv.NewEntry(key, value)
		art.Add(entry)
		entry.DecrRef()
		keys[i] = key
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, vs := art.Search(keys[i%len(keys)])
		if len(vs.Value) == 0 {
			b.Fatalf("missing value")
		}
	}
}

func BenchmarkARTSeek(b *testing.B) {
	const (
		arenaSize = 1 << 20
		keySpace  = 10_000
	)
	art := NewART(arenaSize)
	defer art.DecrRef()
	value := make([]byte, 64)
	keys := make([][]byte, keySpace)
	for i := range keys {
		key := makeARTKey(i)
		entry := kv.NewEntry(key, value)
		art.Add(entry)
		entry.DecrRef()
		keys[i] = key
	}

	it := art.NewIterator(nil)
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

func BenchmarkARTIteratorNext(b *testing.B) {
	const (
		arenaSize = 1 << 20
		keySpace  = 10_000
	)
	art := NewART(arenaSize)
	defer art.DecrRef()
	value := make([]byte, 64)
	for i := range keySpace {
		entry := kv.NewEntry(makeARTKey(i), value)
		art.Add(entry)
		entry.DecrRef()
	}

	it := art.NewIterator(nil)
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
