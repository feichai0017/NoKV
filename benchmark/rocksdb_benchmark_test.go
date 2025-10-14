//go:build benchmark_rocksdb

package benchmark

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/tecbot/gorocksdb"
)

func openRocksDB(b *testing.B) (*gorocksdb.DB, *gorocksdb.Options) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, benchDir)
	if err != nil {
		opts.Destroy()
		b.Fatalf("failed to open RocksDB: %v", err)
	}
	return db, opts
}

func BenchmarkRocksDBWrite(b *testing.B) {
	result := BenchmarkResult{
		Name:      "RocksDB Write",
		Engine:    "RocksDB",
		Operation: "Write",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== RocksDB Write Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db, opts := openRocksDB(b)
	defer func() {
		db.Close()
		opts.Destroy()
	}()

	writeOpts := gorocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()

	data := generateData(b.N)
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		key := data[i]
		value := make([]byte, 100)
		rand.Read(value)
		if err := db.Put(writeOpts, key, value); err != nil {
			b.Fatalf("rocksdb write error: %v", err)
		}
	}
	duration := time.Since(start)

	result.EndTime = time.Now()
	result.TotalDuration = duration
	result.TotalOperations = int64(b.N)
	result.DataSize = float64(b.N*100) / 1024 / 1024
	result.MemoryStats.Allocations = int64(b.N)
	result.MemoryStats.Bytes = int64(b.N * 100)

	fmt.Printf("End Time: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Total Duration: %v\n", duration)
	fmt.Printf("Average Time per Entry: %v\n", duration/time.Duration(b.N))
	fmt.Printf("Total Entries: %d\n", b.N)
	fmt.Printf("Total Data Size: %.2f MB\n", result.DataSize)

	benchmarkResults = append(benchmarkResults, result)
}

func BenchmarkRocksDBRead(b *testing.B) {
	result := BenchmarkResult{
		Name:      "RocksDB Read",
		Engine:    "RocksDB",
		Operation: "Read",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== RocksDB Read Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db, opts := openRocksDB(b)
	defer func() {
		db.Close()
		opts.Destroy()
	}()

	writeOpts := gorocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()

	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()

	num := 100000
	fmt.Printf("Preparing %d test entries\n", num)
	data := generateData(num)
	for i := 0; i < num; i++ {
		key := data[i]
		value := make([]byte, 100)
		rand.Read(value)
		if err := db.Put(writeOpts, key, value); err != nil {
			b.Fatalf("rocksdb preload write error: %v", err)
		}
	}

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		key := data[i%num]
		slice, err := db.Get(readOpts, key)
		if err != nil {
			b.Fatalf("rocksdb read error: %v", err)
		}
		slice.Free()
	}
	duration := time.Since(start)

	result.EndTime = time.Now()
	result.TotalDuration = duration
	result.TotalOperations = int64(b.N)
	result.DataSize = float64(b.N*100) / 1024 / 1024
	result.MemoryStats.Allocations = int64(b.N)
	result.MemoryStats.Bytes = int64(b.N * 100)

	fmt.Printf("End Time: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Total Duration: %v\n", duration)
	fmt.Printf("Average Time per Query: %v\n", duration/time.Duration(b.N))
	fmt.Printf("Total Queries: %d\n", b.N)

	benchmarkResults = append(benchmarkResults, result)
}

func BenchmarkRocksDBBatchWrite(b *testing.B) {
	result := BenchmarkResult{
		Name:      "RocksDB Batch Write",
		Engine:    "RocksDB",
		Operation: "BatchWrite",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== RocksDB Batch Write Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db, opts := openRocksDB(b)
	defer func() {
		db.Close()
		opts.Destroy()
	}()

	writeOpts := gorocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()

	batchSize := 1000
	numBatches := b.N / batchSize
	if numBatches == 0 {
		numBatches = 1
	}
	fmt.Printf("Batch Size: %d\n", batchSize)
	fmt.Printf("Total Batches: %d\n", numBatches)

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < numBatches; i++ {
		wb := gorocksdb.NewWriteBatch()
		for j := 0; j < batchSize; j++ {
			key := []byte(fmt.Sprintf("rocksdb-batch-key-%d-%d", i, j))
			value := make([]byte, 100)
			rand.Read(value)
			wb.Put(key, value)
		}
		if err := db.Write(writeOpts, wb); err != nil {
			wb.Destroy()
			b.Fatalf("rocksdb batch write error: %v", err)
		}
		wb.Destroy()
	}
	duration := time.Since(start)

	result.EndTime = time.Now()
	result.TotalDuration = duration
	result.TotalOperations = int64(numBatches * batchSize)
	result.DataSize = float64(numBatches*batchSize*100) / 1024 / 1024
	result.MemoryStats.Allocations = int64(numBatches * batchSize)
	result.MemoryStats.Bytes = int64(numBatches * batchSize * 100)

	fmt.Printf("End Time: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Total Duration: %v\n", duration)
	fmt.Printf("Average Time per Batch: %v\n", duration/time.Duration(numBatches))
	fmt.Printf("Total Entries: %d\n", numBatches*batchSize)
	fmt.Printf("Total Data Size: %.2f MB\n", result.DataSize)

	benchmarkResults = append(benchmarkResults, result)
}

func BenchmarkRocksDBRangeQuery(b *testing.B) {
	result := BenchmarkResult{
		Name:      "RocksDB Range Query",
		Engine:    "RocksDB",
		Operation: "RangeQuery",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== RocksDB Range Query Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db, opts := openRocksDB(b)
	defer func() {
		db.Close()
		opts.Destroy()
	}()

	writeOpts := gorocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()

	readOpts := gorocksdb.NewDefaultReadOptions()
	readOpts.SetPrefixSameAsStart(true)
	defer readOpts.Destroy()

	num := 100000
	fmt.Printf("Preparing %d test entries\n", num)
	for i := 0; i < num; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := make([]byte, 100)
		rand.Read(value)
		if err := db.Put(writeOpts, key, value); err != nil {
			b.Fatalf("rocksdb preload write error: %v", err)
		}
	}

	b.ResetTimer()
	start := time.Now()
	totalItems := 0
	prefix := []byte("key")
	for i := 0; i < b.N; i++ {
		iter := db.NewIterator(readOpts)
		iter.Seek(prefix)

		count := 0
		for ; iter.Valid(); iter.Next() {
			keySlice := iter.Key()
			if !bytes.HasPrefix(keySlice.Data(), prefix) {
				keySlice.Free()
				break
			}
			valSlice := iter.Value()
			count++
			valSlice.Free()
			keySlice.Free()
		}
		totalItems += count
		iter.Close()
	}
	duration := time.Since(start)

	result.EndTime = time.Now()
	result.TotalDuration = duration
	result.TotalOperations = int64(b.N)
	result.DataSize = float64(totalItems*100) / 1024 / 1024
	result.MemoryStats.Allocations = int64(b.N)
	result.MemoryStats.Bytes = int64(totalItems * 100)

	fmt.Printf("End Time: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Total Duration: %v\n", duration)
	fmt.Printf("Average Time per Query: %v\n", duration/time.Duration(b.N))
	fmt.Printf("Total Queries: %d\n", b.N)
	fmt.Printf("Total Scanned Items: %d\n", totalItems)
	fmt.Printf("Average Items per Query: %d\n", totalItems/b.N)

	benchmarkResults = append(benchmarkResults, result)
}
