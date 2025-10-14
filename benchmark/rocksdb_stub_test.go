//go:build !benchmark_rocksdb

package benchmark

import "testing"

func BenchmarkRocksDBWrite(b *testing.B) {
	b.Skip("RocksDB benchmark requires build tag 'benchmark_rocksdb' and librocksdb")
}

func BenchmarkRocksDBRead(b *testing.B) {
	b.Skip("RocksDB benchmark requires build tag 'benchmark_rocksdb' and librocksdb")
}

func BenchmarkRocksDBBatchWrite(b *testing.B) {
	b.Skip("RocksDB benchmark requires build tag 'benchmark_rocksdb' and librocksdb")
}

func BenchmarkRocksDBRangeQuery(b *testing.B) {
	b.Skip("RocksDB benchmark requires build tag 'benchmark_rocksdb' and librocksdb")
}
