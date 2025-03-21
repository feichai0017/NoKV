package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

var (
	benchDir = "benchmark_data"
	opt      = &NoKV.Options{
		WorkDir:             benchDir,
		MemTableSize:        64 << 20, // 64MB
		SSTableMaxSz:        2 << 30,  // 2GB
		ValueLogFileSize:    1 << 30,  // 1GB
		ValueLogMaxEntries:  100000,
		ValueThreshold:      1 << 20, // 1MB
		MaxBatchCount:       10000,
		MaxBatchSize:        10 << 20, // 10MB
		VerifyValueChecksum: true,
		DetectConflicts:     true,
	}
	benchmarkResults []BenchmarkResult
)

// Clear benchmark directory
func clearBenchDir() {
	fmt.Printf("Clearing benchmark directory: %s\n", benchDir)
	os.RemoveAll(benchDir)
	os.MkdirAll(benchDir, 0755)
}

// Generate test data
func generateData(num int) [][]byte {
	fmt.Printf("Generating %d test data entries\n", num)
	data := make([][]byte, num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("key%d", i)
		value := make([]byte, 100)
		rand.Read(value)
		data[i] = []byte(key)
	}
	return data
}

// NoKV Write Benchmark
func BenchmarkNoKVWrite(b *testing.B) {
	result := BenchmarkResult{
		Name:      "NoKV Write",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== NoKV Write Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	data := generateData(b.N)
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		key := data[i]
		value := make([]byte, 100)
		rand.Read(value)
		e := utils.NewEntry(key, value)
		if err := db.Set(e); err != nil {
			b.Fatal(err)
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

// Badger Write Benchmark
func BenchmarkBadgerWrite(b *testing.B) {
	result := BenchmarkResult{
		Name:      "Badger Write",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== Badger Write Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	opts := badger.DefaultOptions(benchDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	data := generateData(b.N)
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		key := data[i]
		value := make([]byte, 100)
		rand.Read(value)
		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		if err != nil {
			b.Fatal(err)
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

// NoKV Read Benchmark
func BenchmarkNoKVRead(b *testing.B) {
	result := BenchmarkResult{
		Name:      "NoKV Read",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== NoKV Read Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	num := 100000
	fmt.Printf("Preparing %d test entries\n", num)
	data := generateData(num)
	for i := 0; i < num; i++ {
		key := data[i]
		value := make([]byte, 100)
		rand.Read(value)
		e := utils.NewEntry(key, value)
		if err := db.Set(e); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		key := data[i%num]
		if _, err := db.Get(key); err != nil {
			b.Fatal(err)
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
	fmt.Printf("Average Time per Query: %v\n", duration/time.Duration(b.N))
	fmt.Printf("Total Queries: %d\n", b.N)

	benchmarkResults = append(benchmarkResults, result)
}

// Badger Read Benchmark
func BenchmarkBadgerRead(b *testing.B) {
	result := BenchmarkResult{
		Name:      "Badger Read",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== Badger Read Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	opts := badger.DefaultOptions(benchDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	num := 100000
	fmt.Printf("Preparing %d test entries\n", num)
	data := generateData(num)
	for i := 0; i < num; i++ {
		key := data[i]
		value := make([]byte, 100)
		rand.Read(value)
		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		key := data[i%num]
		err := db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			return err
		})
		if err != nil {
			b.Fatal(err)
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
	fmt.Printf("Average Time per Query: %v\n", duration/time.Duration(b.N))
	fmt.Printf("Total Queries: %d\n", b.N)

	benchmarkResults = append(benchmarkResults, result)
}

// NoKV Batch Write Benchmark
func BenchmarkNoKVBatchWrite(b *testing.B) {
	result := BenchmarkResult{
		Name:      "NoKV Batch Write",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== NoKV Batch Write Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	batchSize := 1000
	numBatches := b.N / batchSize
	if numBatches == 0 {
		numBatches = 1
	}
	fmt.Printf("Batch Size: %d\n", batchSize)
	fmt.Printf("Total Batches: %d\n", numBatches)

	data := generateData(batchSize)
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < numBatches; i++ {
		entries := make([]*utils.Entry, batchSize)
		for j := 0; j < batchSize; j++ {
			key := data[j]
			value := make([]byte, 100)
			rand.Read(value)
			entries[j] = utils.NewEntry(key, value)
		}
		for _, e := range entries {
			if err := db.Set(e); err != nil {
				b.Fatal(err)
			}
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
	fmt.Printf("Average Time per Batch: %v\n", duration/time.Duration(numBatches))
	fmt.Printf("Total Entries: %d\n", b.N)
	fmt.Printf("Total Data Size: %.2f MB\n", result.DataSize)

	benchmarkResults = append(benchmarkResults, result)
}

// Badger Batch Write Benchmark
func BenchmarkBadgerBatchWrite(b *testing.B) {
	result := BenchmarkResult{
		Name:      "Badger Batch Write",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== Badger Batch Write Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	opts := badger.DefaultOptions(benchDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	batchSize := 1000
	numBatches := b.N / batchSize
	if numBatches == 0 {
		numBatches = 1
	}
	fmt.Printf("Batch Size: %d\n", batchSize)
	fmt.Printf("Total Batches: %d\n", numBatches)

	data := generateData(batchSize)
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < numBatches; i++ {
		err := db.Update(func(txn *badger.Txn) error {
			for j := 0; j < batchSize; j++ {
				key := data[j]
				value := make([]byte, 100)
				rand.Read(value)
				if err := txn.Set(key, value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
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
	fmt.Printf("Average Time per Batch: %v\n", duration/time.Duration(numBatches))
	fmt.Printf("Total Entries: %d\n", b.N)
	fmt.Printf("Total Data Size: %.2f MB\n", result.DataSize)

	benchmarkResults = append(benchmarkResults, result)
}

// NoKV Range Query Benchmark
func BenchmarkNoKVRangeQuery(b *testing.B) {
	result := BenchmarkResult{
		Name:      "NoKV Range Query",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== NoKV Range Query Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	num := 100000
	fmt.Printf("Preparing %d test entries\n", num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("key%d", i)
		value := make([]byte, 100)
		rand.Read(value)
		e := utils.NewEntry([]byte(key), value)
		if err := db.Set(e); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	start := time.Now()
	totalItems := 0
	for i := 0; i < b.N; i++ {
		iter := db.NewIterator(&utils.Options{
			Prefix: []byte("key"),
			IsAsc:  true,
		})
		defer iter.Close()

		count := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			_ = iter.Item()
			count++
		}
		totalItems += count
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

// Badger Range Query Benchmark
func BenchmarkBadgerRangeQuery(b *testing.B) {
	result := BenchmarkResult{
		Name:      "Badger Range Query",
		StartTime: time.Now(),
	}
	fmt.Printf("\n=== Badger Range Query Benchmark ===\n")
	fmt.Printf("Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))

	clearBenchDir()
	opts := badger.DefaultOptions(benchDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	num := 100000
	fmt.Printf("Preparing %d test entries\n", num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("key%d", i)
		value := make([]byte, 100)
		rand.Read(value)
		err := db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(key), value)
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	start := time.Now()
	totalItems := 0
	for i := 0; i < b.N; i++ {
		err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte("key")
			it := txn.NewIterator(opts)
			defer it.Close()

			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				_ = it.Item()
				count++
			}
			totalItems += count
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
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

// Test benchmark results
func TestBenchmarkResults(t *testing.T) {
	fmt.Printf("\n=== Starting Benchmark Tests ===\n")
	fmt.Printf("Test Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("Test Directory: %s\n", benchDir)
	fmt.Printf("Configuration:\n")
	fmt.Printf("  MemTableSize: %d MB\n", opt.MemTableSize>>20)
	fmt.Printf("  SSTableMaxSz: %d GB\n", opt.SSTableMaxSz>>30)
	fmt.Printf("  ValueLogFileSize: %d GB\n", opt.ValueLogFileSize>>30)
	fmt.Printf("  ValueThreshold: %d MB\n", opt.ValueThreshold>>20)
	fmt.Printf("  MaxBatchCount: %d\n", opt.MaxBatchCount)
	fmt.Printf("  MaxBatchSize: %d MB\n", opt.MaxBatchSize>>20)
	fmt.Printf("  VerifyValueChecksum: %v\n", opt.VerifyValueChecksum)
	fmt.Printf("  DetectConflicts: %v\n", opt.DetectConflicts)
	fmt.Printf("\n")

	// Run all benchmarks
	testing.Benchmark(BenchmarkNoKVWrite)
	testing.Benchmark(BenchmarkBadgerWrite)
	testing.Benchmark(BenchmarkNoKVRead)
	testing.Benchmark(BenchmarkBadgerRead)
	testing.Benchmark(BenchmarkNoKVBatchWrite)
	testing.Benchmark(BenchmarkBadgerBatchWrite)
	testing.Benchmark(BenchmarkNoKVRangeQuery)
	testing.Benchmark(BenchmarkBadgerRangeQuery)

	// Write results to file
	if err := WriteResults(benchmarkResults); err != nil {
		t.Errorf("Failed to write benchmark results: %v", err)
	}
}
