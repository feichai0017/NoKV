package vlog

import (
	"encoding/binary"
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func newBenchManager(b *testing.B) *Manager {
	b.Helper()
	mgr, err := Open(Config{Dir: b.TempDir(), MaxSize: 1 << 28})
	if err != nil {
		b.Fatalf("open manager: %v", err)
	}
	b.Cleanup(func() {
		_ = mgr.Close()
	})
	return mgr
}

func makeVLogEntries(batchSize int, valueSize int) []*kv.Entry {
	entries := make([]*kv.Entry, batchSize)
	value := make([]byte, valueSize)
	for i := range batchSize {
		key := make([]byte, 16)
		copy(key, "benchkey")
		binary.LittleEndian.PutUint64(key[8:], uint64(i))
		internal := kv.InternalKey(kv.CFDefault, key, uint64(i+1))
		entries[i] = &kv.Entry{Key: internal, Value: value, CF: kv.CFDefault, Version: uint64(i + 1)}
	}
	return entries
}

func BenchmarkVLogAppendEntries(b *testing.B) {
	mgr := newBenchManager(b)
	entries := makeVLogEntries(32, 256)
	b.ReportAllocs()
	b.SetBytes(int64(32 * 256))

	for b.Loop() {
		if _, err := mgr.AppendEntries(entries, nil); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func BenchmarkVLogReadValue(b *testing.B) {
	mgr := newBenchManager(b)
	entries := makeVLogEntries(128, 256)
	ptrs, err := mgr.AppendEntries(entries, nil)
	if err != nil {
		b.Fatalf("append: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		ptr := &ptrs[i%len(ptrs)]
		val, unlock, err := mgr.ReadValue(ptr, ReadOptions{Mode: ReadModeCopy})
		if unlock != nil {
			unlock()
		}
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		if len(val) == 0 {
			b.Fatalf("empty read")
		}
	}
}
