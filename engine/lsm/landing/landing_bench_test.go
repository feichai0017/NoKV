package landing

import (
	"fmt"
	"testing"
)

// BenchmarkBufferAdd measures the per-Add cost as the buffer grows. Add
// happens on every L0→landing promotion, so its cost shows up under sustained
// flush pressure.
func BenchmarkBufferAdd(b *testing.B) {
	tables := make([]*fakeTable, b.N)
	for i := range tables {
		tables[i] = newTable(uint64(i), fmt.Sprintf("k%05d", i), 1, "v")
	}
	var buf Buffer[*fakeTable]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Add(tables[i])
	}
}

// BenchmarkBufferAddBatch measures batch insertion which only rebuilds the
// affected shards once per call.
func BenchmarkBufferAddBatch(b *testing.B) {
	const size = 64
	var buf Buffer[*fakeTable]
	for i := 0; i < b.N; i++ {
		batch := make([]*fakeTable, size)
		for j := range batch {
			batch[j] = newTable(uint64(i*size+j), fmt.Sprintf("k%08d", i*size+j), 1, "v")
		}
		b.StartTimer()
		buf.AddBatch(batch)
		b.StopTimer()
	}
}

// BenchmarkBufferShardViews measures the per-shard summary gather used by
// the compaction picker on every cycle.
func BenchmarkBufferShardViews(b *testing.B) {
	var buf Buffer[*fakeTable]
	for i := range 256 {
		buf.Add(newTable(uint64(i), fmt.Sprintf("k%05d", i), 1, "v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buf.ShardViews()
	}
}

// BenchmarkBufferSearch measures the read path that walks shard prefix
// indexes.
func BenchmarkBufferSearch(b *testing.B) {
	var buf Buffer[*fakeTable]
	for i := range 256 {
		buf.Add(newTable(uint64(i), fmt.Sprintf("k%05d", i), uint64(i+1), "v"))
	}
	buf.SortShards()
	probe := ikey("k00128", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, _, err := buf.Search(probe, 0)
		if err == nil && entry != nil {
			entry.DecrRef()
		}
	}
}
