package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"text/tabwriter"
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

type readScenarioResult struct {
	Name string
	QPS  float64
	P99  time.Duration
}

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
	for i := range num {
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
		Engine:    "NoKV",
		Operation: "Write",
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
		Engine:    "Badger",
		Operation: "Write",
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

func runCacheReadScenario(t *testing.T, label string, cacheSize, bloomSize int) readScenarioResult {
	t.Helper()
	clearBenchDir()
	cfg := *opt
	cfg.BlockCacheSize = cacheSize
	cfg.BloomCacheSize = bloomSize
	if cacheSize == 0 {
		cfg.BlockCacheHotFraction = 0
	}
	db := NoKV.Open(&cfg)
	defer db.Close()

	const numKeys = 5000
	keys := generateData(numKeys)
	value := make([]byte, 128)
	for i := 0; i < numKeys; i++ {
		rand.Read(value)
		if err := db.Set(utils.NewEntry(keys[i], value)); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	// Warm up to populate caches before measurement.
	for i := 0; i < 1000; i++ {
		if _, err := db.Get(keys[rand.Intn(numKeys)]); err != nil {
			t.Fatalf("warmup get: %v", err)
		}
	}

	const reads = 2000
	durations := make([]time.Duration, reads)
	start := time.Now()
	for i := 0; i < reads; i++ {
		key := keys[i%numKeys]
		opStart := time.Now()
		if _, err := db.Get(key); err != nil {
			t.Fatalf("get: %v", err)
		}
		durations[i] = time.Since(opStart)
	}
	total := time.Since(start)
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p99Index := int(float64(len(durations))*0.99) - 1
	if p99Index < 0 {
		p99Index = 0
	}
	if p99Index >= len(durations) {
		p99Index = len(durations) - 1
	}
	return readScenarioResult{
		Name: label,
		QPS:  float64(reads) / total.Seconds(),
		P99:  durations[p99Index],
	}
}

func TestCacheReadScenarios(t *testing.T) {
	enabled := runCacheReadScenario(t, "cache-enabled", opt.BlockCacheSize, opt.BloomCacheSize)
	disabled := runCacheReadScenario(t, "cache-disabled", 0, 0)
	t.Logf("%s: QPS=%.2f P99=%s", enabled.Name, enabled.QPS, enabled.P99)
	t.Logf("%s: QPS=%.2f P99=%s", disabled.Name, disabled.QPS, disabled.P99)
}


// NoKV Read Benchmark
func BenchmarkNoKVRead(b *testing.B) {
	result := BenchmarkResult{
		Name:      "NoKV Read",
		Engine:    "NoKV",
		Operation: "Read",
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
		Engine:    "Badger",
		Operation: "Read",
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
		Engine:    "NoKV",
		Operation: "BatchWrite",
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

// Badger Batch Write Benchmark
func BenchmarkBadgerBatchWrite(b *testing.B) {
	result := BenchmarkResult{
		Name:      "Badger Batch Write",
		Engine:    "Badger",
		Operation: "BatchWrite",
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

// NoKV Range Query Benchmark
func BenchmarkNoKVRangeQuery(b *testing.B) {
	result := BenchmarkResult{
		Name:      "NoKV Range Query",
		Engine:    "NoKV",
		Operation: "RangeQuery",
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
		Engine:    "Badger",
		Operation: "RangeQuery",
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
	testing.Benchmark(BenchmarkRocksDBWrite)
	testing.Benchmark(BenchmarkRocksDBRead)
	testing.Benchmark(BenchmarkRocksDBBatchWrite)
	testing.Benchmark(BenchmarkRocksDBRangeQuery)

	if len(benchmarkResults) > 0 {
		fmt.Printf("\nBenchmark Summary (ops/sec, latency):\n")
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		writeSummaryTable(tw, benchmarkResults)
		fmt.Println()
	}

	// Write results to file
	if err := WriteResults(benchmarkResults); err != nil {
		t.Errorf("Failed to write benchmark results: %v", err)
	}
}
