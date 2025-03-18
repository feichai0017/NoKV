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
)

// 清理测试目录
func clearBenchDir() {
	os.RemoveAll(benchDir)
	os.MkdirAll(benchDir, 0755)
}

// 生成测试数据
func generateData(num int) [][]byte {
	data := make([][]byte, num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("key%d", i)
		value := make([]byte, 100)
		rand.Read(value)
		data[i] = []byte(key)
	}
	return data
}

// NoKV写入基准测试
func BenchmarkNoKVWrite(b *testing.B) {
	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	data := generateData(b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := data[i]
		value := make([]byte, 100)
		rand.Read(value)
		e := utils.NewEntry(key, value)
		if err := db.Set(e); err != nil {
			b.Fatal(err)
		}
	}
}

// Badger写入基准测试
func BenchmarkBadgerWrite(b *testing.B) {
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
}

// NoKV读取基准测试
func BenchmarkNoKVRead(b *testing.B) {
	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	num := 100000
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
	for i := 0; i < b.N; i++ {
		key := data[i%num]
		if _, err := db.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

// Badger读取基准测试
func BenchmarkBadgerRead(b *testing.B) {
	clearBenchDir()
	opts := badger.DefaultOptions(benchDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	num := 100000
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
}

// NoKV批量写入基准测试
func BenchmarkNoKVBatchWrite(b *testing.B) {
	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	batchSize := 1000
	numBatches := b.N / batchSize
	data := generateData(batchSize)
	b.ResetTimer()

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
}

// Badger批量写入基准测试
func BenchmarkBadgerBatchWrite(b *testing.B) {
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
	data := generateData(batchSize)
	b.ResetTimer()

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
}

// NoKV范围查询基准测试
func BenchmarkNoKVRangeQuery(b *testing.B) {
	clearBenchDir()
	db := NoKV.Open(opt)
	defer db.Close()

	num := 100000
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
	for i := 0; i < b.N; i++ {
		iter := db.NewIterator(&utils.Options{
			Prefix: []byte("key"),
			IsAsc:  true,
		})
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			_ = iter.Item()
		}
	}
}

// Badger范围查询基准测试
func BenchmarkBadgerRangeQuery(b *testing.B) {
	clearBenchDir()
	opts := badger.DefaultOptions(benchDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	num := 100000
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
	for i := 0; i < b.N; i++ {
		err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte("key")
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				_ = it.Item()
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// 测试结果记录
func TestBenchmarkResults(t *testing.T) {
	results := make(map[string]testing.BenchmarkResult)

	// 写入测试
	results["NoKVWrite"] = testing.Benchmark(BenchmarkNoKVWrite)
	results["BadgerWrite"] = testing.Benchmark(BenchmarkBadgerWrite)
	results["NoKVBatchWrite"] = testing.Benchmark(BenchmarkNoKVBatchWrite)
	results["BadgerBatchWrite"] = testing.Benchmark(BenchmarkBadgerBatchWrite)

	// 读取测试
	results["NoKVRead"] = testing.Benchmark(BenchmarkNoKVRead)
	results["BadgerRead"] = testing.Benchmark(BenchmarkBadgerRead)
	results["NoKVRangeQuery"] = testing.Benchmark(BenchmarkNoKVRangeQuery)
	results["BadgerRangeQuery"] = testing.Benchmark(BenchmarkBadgerRangeQuery)

	// 输出结果
	fmt.Println("\n=== Benchmark Results ===")
	fmt.Printf("Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	fmt.Println("\nWrite Operations:")
	fmt.Printf("NoKV Write: %s\n", results["NoKVWrite"])
	fmt.Printf("Badger Write: %s\n", results["BadgerWrite"])
	fmt.Printf("NoKV Batch Write: %s\n", results["NoKVBatchWrite"])
	fmt.Printf("Badger Batch Write: %s\n", results["BadgerBatchWrite"])

	fmt.Println("\nRead Operations:")
	fmt.Printf("NoKV Read: %s\n", results["NoKVRead"])
	fmt.Printf("Badger Read: %s\n", results["BadgerRead"])
	fmt.Printf("NoKV Range Query: %s\n", results["NoKVRangeQuery"])
	fmt.Printf("Badger Range Query: %s\n", results["BadgerRangeQuery"])

	// 保存结果到文件
	f, err := os.OpenFile("benchmark_results.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	fmt.Fprintf(f, "\n=== Benchmark Results ===\n")
	fmt.Fprintf(f, "Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	fmt.Fprintf(f, "\nWrite Operations:\n")
	fmt.Fprintf(f, "NoKV Write: %s\n", results["NoKVWrite"])
	fmt.Fprintf(f, "Badger Write: %s\n", results["BadgerWrite"])
	fmt.Fprintf(f, "NoKV Batch Write: %s\n", results["NoKVBatchWrite"])
	fmt.Fprintf(f, "Badger Batch Write: %s\n", results["BadgerBatchWrite"])

	fmt.Fprintf(f, "\nRead Operations:\n")
	fmt.Fprintf(f, "NoKV Read: %s\n", results["NoKVRead"])
	fmt.Fprintf(f, "Badger Read: %s\n", results["BadgerRead"])
	fmt.Fprintf(f, "NoKV Range Query: %s\n", results["NoKVRangeQuery"])
	fmt.Fprintf(f, "Badger Range Query: %s\n", results["BadgerRangeQuery"])
}
