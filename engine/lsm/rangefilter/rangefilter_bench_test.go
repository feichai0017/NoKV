package rangefilter

import (
	"fmt"
	"testing"
)

// BenchmarkBuild measures the cost of constructing a per-level filter. Build
// runs every time the level table set changes, so it sits on the compaction
// commit hot path.
func BenchmarkBuild(b *testing.B) {
	for _, n := range []int{4, 16, 64, 256} {
		tables := makeNonOverlappingTables(n)
		b.Run(fmt.Sprintf("tables_%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = Build(1, tables)
			}
		})
	}
}

// BenchmarkTableForPoint measures the binary-search lookup that happens on
// every read against a non-overlapping level (L1+).
func BenchmarkTableForPoint(b *testing.B) {
	tables := makeNonOverlappingTables(256)
	f := Build(1, tables)
	probe := ikey("k00128", 5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.TableForPoint(probe)
	}
}

// BenchmarkTablesForBounds measures the bounded scan path used by iterators.
func BenchmarkTablesForBounds(b *testing.B) {
	tables := makeNonOverlappingTables(256)
	f := Build(1, tables)
	lo := ikey("k00100", 1)
	hi := ikey("k00150", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.TablesForBounds(lo, hi)
	}
}

func makeNonOverlappingTables(n int) []*fakeTable {
	out := make([]*fakeTable, n)
	for i := range out {
		out[i] = &fakeTable{
			id:  uint64(i + 1),
			min: ikey(fmt.Sprintf("k%05d", i*2), 10),
			max: ikey(fmt.Sprintf("k%05d", i*2+1), 1),
		}
	}
	return out
}
