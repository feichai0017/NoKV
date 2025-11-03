//go:build !cgo || !benchmark_rocksdb

package benchmark

import "fmt"

type rocksdbEngine struct{}

func newRocksDBEngine(ycsbEngineOptions) ycsbEngine {
	return &rocksdbEngine{}
}

func (e *rocksdbEngine) Name() string { return "RocksDB" }
func (e *rocksdbEngine) Open(bool) error {
	return fmt.Errorf("rocksdb support requires cgo and the benchmark_rocksdb build tag")
}
func (e *rocksdbEngine) Close() error { return nil }
func (e *rocksdbEngine) Read([]byte) error {
	return fmt.Errorf("rocksdb support requires cgo and the benchmark_rocksdb build tag")
}
func (e *rocksdbEngine) Insert([]byte, []byte) error {
	return fmt.Errorf("rocksdb support requires cgo and the benchmark_rocksdb build tag")
}
func (e *rocksdbEngine) Update([]byte, []byte) error {
	return fmt.Errorf("rocksdb support requires cgo and the benchmark_rocksdb build tag")
}
func (e *rocksdbEngine) Scan([]byte, int) (int, error) {
	return 0, fmt.Errorf("rocksdb support requires cgo and the benchmark_rocksdb build tag")
}
