package benchmark

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"text/tabwriter"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	NoKV "github.com/feichai0017/NoKV"
	unix "golang.org/x/sys/unix"
)

// ------------------------------ Flags & Config ------------------------------

var (
	fBenchDir   = flag.String("benchdir", "benchmark_data", "benchmark working directory")
	fSeed       = flag.Int64("seed", 42, "random seed for data generation")
	fSyncWrites = flag.Bool("sync", false, "sync writes (fsync) for both engines")
	fConc       = flag.Int("conc", 0, "concurrency (0 => use GOMAXPROCS)")
	fRounds     = flag.Int("rounds", 1, "repeat the whole suite N rounds")

	fValueSize      = flag.Int("valsz", 1000, "value size (bytes)")
	fPreloadEntries = flag.Int("preload", 1000000, "preload entries for read/range")
	fWriteOps       = flag.Int("write_ops", 5000000, "number of single-write ops")
	fReadOps        = flag.Int("read_ops", 20000000, "number of read ops")
	fBatchEntries   = flag.Int("batch_entries", 20000000, "total entries for batch write")
	fBatchSize      = flag.Int("batch_size", 10000, "batch size per transaction")
	fRangeQueries   = flag.Int("range_q", 20000, "number of range queries")
	fRangeWindow    = flag.Int("range_win", 1000, "range window per query (items)")

	fMode = flag.String("mode", "both", "read/range mode: warm|cold|both")

	fBadgerBlockMB     = flag.Int("badger_block_cache_mb", 256, "Badger block cache size (MB)")
	fBadgerIndexMB     = flag.Int("badger_index_cache_mb", 128, "Badger index cache size (MB)")
	fBadgerCompression = flag.String("badger_compression", "none", "Badger compression: none|snappy|zstd")

	fValueThreshold = flag.Int("value_threshold", 32, "value size threshold (bytes) before spilling to value log (applied to both engines)")
	fDropCacheMode  = flag.String("drop_cache", "none", "drop-cache mode for cold workloads: none|direct|sudo")
	fDropCacheWait  = flag.Duration("drop_cache_wait", 2*time.Second, "sleep duration after dropping caches before continuing cold workloads")
)

type workloadConfig struct {
	ValueSize      int
	PreloadEntries int
	WriteOps       int
	ReadOps        int
	BatchEntries   int
	BatchSize      int
	RangeQueries   int
	RangeWindow    int
	Seed           int64
	SyncWrites     bool
	Conc           int
	Mode           string
	Dir            string
	BadgerBlockMB  int
	BadgerIndexMB  int
	BadgerComp     string
	ValueThreshold int
	DropCacheMode  string
	DropCacheWait  time.Duration
}

var wl workloadConfig

const benchmarkEnvKey = "NOKV_RUN_BENCHMARKS"

var allResults []BenchmarkResult

// ------------------------------ Utilities ------------------------------

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func ensureCleanDir(dir string) error {
	_ = os.RemoveAll(dir)
	return os.MkdirAll(dir, 0o755)
}

func engineDir(engine, suffix string) string {
	return filepath.Join(wl.Dir, fmt.Sprintf("%s_%s", engine, suffix))
}

func concurrency() int {
	if *fConc > 0 {
		return *fConc
	}
	return runtime.GOMAXPROCS(0)
}

func parallelDo(t *testing.T, nWorkers, total int, work func(start, end int) error) {
	t.Helper()
	if nWorkers <= 1 || total <= 1 {
		if err := work(0, total); err != nil {
			t.Fatalf("work failed: %v", err)
		}
		return
	}
	per := (total + nWorkers - 1) / nWorkers
	var wg sync.WaitGroup
	errCh := make(chan error, nWorkers)
	for w := range nWorkers {
		start := w * per
		end := start + per
		if start >= total {
			break
		}
		if end > total {
			end = total
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			if err := work(s, e); err != nil {
				errCh <- err
			}
		}(start, end)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("parallel work failed: %v", err)
		}
	}
}

func nowStr() string { return time.Now().Format("2006-01-02 15:04:05") }

// ------------------------------ Engine Openers ------------------------------

func openBadgerAt(dir string, clean bool) (*badger.DB, error) {
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return nil, fmt.Errorf("badger ensure dir: %w", err)
		}
	} else {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("badger open dir: %w", err)
		}
	}
	comp := options.None
	switch strings.ToLower(wl.BadgerComp) {
	case "none":
		comp = options.None
	case "snappy":
		comp = options.Snappy
	case "zstd":
		comp = options.ZSTD
	default:
		comp = options.None
	}
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(wl.SyncWrites).
		WithCompression(comp).
		WithBlockCacheSize(int64(wl.BadgerBlockMB) << 20).
		WithIndexCacheSize(int64(wl.BadgerIndexMB) << 20).
		WithValueThreshold(int64(wl.ValueThreshold))
	return badger.Open(opts)
}

func openNoKVAt(dir string, clean bool) (*NoKV.DB, error) {
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return nil, fmt.Errorf("nokv ensure dir: %w", err)
		}
	} else {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("nokv open dir: %w", err)
		}
	}
	maxBatchCount := 2 * wl.BatchSize
	if maxBatchCount < 10000 {
		maxBatchCount = 10000
	}
	estimatedBatchBytes := int64(wl.BatchSize) * int64(wl.ValueSize)
	if estimatedBatchBytes < 0 {
		estimatedBatchBytes = 0
	}
	maxBatchSize := int64(128 << 20) // 128 MiB default upper bound
	if est := estimatedBatchBytes * 2; est > maxBatchSize {
		maxBatchSize = est
	}
	opt := &NoKV.Options{
		WorkDir:             dir,
		MemTableSize:        64 << 20,
		SSTableMaxSz:        1 << 30,
		ValueLogFileSize:    1 << 30,
		ValueLogMaxEntries:  100000,
		ValueThreshold:      int64(wl.ValueThreshold),
		MaxBatchCount:       int64(maxBatchCount),
		MaxBatchSize:        maxBatchSize,
		VerifyValueChecksum: true,
		DetectConflicts:     false,
		SyncWrites:          wl.SyncWrites,
	}
	return NoKV.Open(opt), nil
}

func dropCachesDirect() error {
	unix.Sync()
	f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open drop_caches: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString("3\n"); err != nil {
		return fmt.Errorf("write drop_caches: %w", err)
	}
	return nil
}

func dropCachesSudo() error {
	cmd := exec.Command("sudo", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches")
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		fmt.Printf("%s\n", strings.TrimSpace(string(out)))
	}
	if err != nil {
		return fmt.Errorf("sudo drop caches: %w", err)
	}
	return nil
}

// maybeDropCaches executes the configured strategy to flush the OS cache before cold runs.
func maybeDropCaches(t *testing.T, label string) {
	mode := strings.ToLower(strings.TrimSpace(wl.DropCacheMode))
	if mode == "" || mode == "none" {
		return
	}
	fmt.Printf(">>> drop cache (%s): mode=%s\n", label, mode)
	var err error
	switch mode {
	case "direct":
		err = dropCachesDirect()
	case "sudo":
		err = dropCachesSudo()
	default:
		t.Fatalf("unknown drop_cache mode: %s", mode)
	}
	if err != nil {
		t.Logf("drop cache (%s) failed: %v", label, err)
		return
	}
	if wl.DropCacheWait > 0 {
		time.Sleep(wl.DropCacheWait)
	}
}

// ------------------------------ Streaming Preload ------------------------------

func preloadNoKV(t *testing.T, db *NoKV.DB, total, batch, valSize int, seed int64, keyPrefix string) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	buf := make([]byte, valSize)
	for i := 0; i < total; i += batch {
		end := min(i+batch, total)
		err := db.Update(func(txn *NoKV.Txn) error {
			for j := i; j < end; j++ {
				key := fmt.Appendf(nil, "%s%016d", keyPrefix, j)
				if _, err := rng.Read(buf); err != nil {
					return err
				}
				val := append([]byte(nil), buf...)
				if err := txn.Set(key, val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("NoKV preload failed: %v", err)
		}
	}
}

func preloadBadger(t *testing.T, db *badger.DB, total, batch, valSize int, seed int64, keyPrefix string) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	buf := make([]byte, valSize)
	for i := 0; i < total; i += batch {
		end := min(i+batch, total)
		err := db.Update(func(txn *badger.Txn) error {
			for j := i; j < end; j++ {
				key := fmt.Appendf(nil, "%s%016d", keyPrefix, j)
				if _, err := rng.Read(buf); err != nil {
					return err
				}
				val := append([]byte(nil), buf...)
				if err := txn.Set(key, val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Badger preload failed: %v", err)
		}
	}
}

// ------------------------------ Bench Workloads ------------------------------

func benchNoKVWrite(t *testing.T) {
	t.Helper()
	fmt.Printf("\n=== NoKV Write (sync=%v) ===\n", wl.SyncWrites)
	dir := engineDir("nokv", "write")
	db, err := openNoKVAt(dir, true)
	if err != nil {
		t.Fatalf("open nokv: %v", err)
	}
	defer db.Close()

	start := time.Now()
	W := concurrency()
	total := wl.WriteOps
	parallelDo(t, W, total, func(s, e int) error {
		rng := rand.New(rand.NewSource(wl.Seed + int64(s)))
		val := make([]byte, wl.ValueSize)
		for i := s; i < e; i++ {
			if _, err := rng.Read(val); err != nil {
				return fmt.Errorf("rand: %w", err)
			}
			key := fmt.Appendf(nil, "key-w-%016d", i)
			if err := db.Update(func(txn *NoKV.Txn) error {
				return txn.Set(key, append([]byte(nil), val...))
			}); err != nil {
				return fmt.Errorf("nokv write: %w", err)
			}
		}
		return nil
	})
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "NoKV Write",
		Engine:          "NoKV",
		Operation:       "Write",
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(total),
		DataSize:        float64(total*wl.ValueSize) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

func benchBadgerWrite(t *testing.T) {
	t.Helper()
	fmt.Printf("\n=== Badger Write (sync=%v) ===\n", wl.SyncWrites)
	dir := engineDir("badger", "write")
	db, err := openBadgerAt(dir, true)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	defer db.Close()

	start := time.Now()
	W := concurrency()
	total := wl.WriteOps
	parallelDo(t, W, total, func(s, e int) error {
		rng := rand.New(rand.NewSource(wl.Seed + int64(s)))
		val := make([]byte, wl.ValueSize)
		for i := s; i < e; i++ {
			if _, err := rng.Read(val); err != nil {
				return fmt.Errorf("rand: %w", err)
			}
			key := fmt.Appendf(nil, "key-w-%016d", i)
			if err := db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, append([]byte(nil), val...))
			}); err != nil {
				return fmt.Errorf("badger write: %w", err)
			}
		}
		return nil
	})
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "Badger Write",
		Engine:          "Badger",
		Operation:       "Write",
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(total),
		DataSize:        float64(total*wl.ValueSize) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

func benchNoKVBatchWrite(t *testing.T) {
	t.Helper()
	fmt.Printf("\n=== NoKV BatchWrite (sync=%v, batch=%d) ===\n", wl.SyncWrites, wl.BatchSize)
	dir := engineDir("nokv", "batchwrite")
	db, err := openNoKVAt(dir, true)
	if err != nil {
		t.Fatalf("open nokv: %v", err)
	}
	defer db.Close()

	start := time.Now()
	total := wl.BatchEntries
	batch := wl.BatchSize
	if batch <= 0 {
		batch = total
	}
	rng := rand.New(rand.NewSource(wl.Seed))
	buf := make([]byte, wl.ValueSize)

	for i := 0; i < total; i += batch {
		end := min(i+batch, total)
		err := db.Update(func(txn *NoKV.Txn) error {
			for j := i; j < end; j++ {
				key := fmt.Appendf(nil, "key-b-%016d", j)
				if _, err := rng.Read(buf); err != nil {
					return err
				}
				val := append([]byte(nil), buf...)
				if err := txn.Set(key, val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("nokv batch write: %v", err)
		}
	}
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "NoKV BatchWrite",
		Engine:          "NoKV",
		Operation:       "BatchWrite",
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(total),
		DataSize:        float64(total*wl.ValueSize) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

func benchBadgerBatchWrite(t *testing.T) {
	t.Helper()
	fmt.Printf("\n=== Badger BatchWrite (sync=%v, batch=%d) ===\n", wl.SyncWrites, wl.BatchSize)
	dir := engineDir("badger", "batchwrite")
	db, err := openBadgerAt(dir, true)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	defer db.Close()

	start := time.Now()
	total := wl.BatchEntries
	batch := wl.BatchSize
	if batch <= 0 {
		batch = total
	}
	rng := rand.New(rand.NewSource(wl.Seed))
	buf := make([]byte, wl.ValueSize)

	for i := 0; i < total; i += batch {
		end := min(i+batch, total)
		err := db.Update(func(txn *badger.Txn) error {
			for j := i; j < end; j++ {
				key := fmt.Appendf(nil, "key-b-%016d", j)
				if _, err := rng.Read(buf); err != nil {
					return err
				}
				val := append([]byte(nil), buf...)
				if err := txn.Set(key, val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("badger batch write: %v", err)
		}
	}
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "Badger BatchWrite",
		Engine:          "Badger",
		Operation:       "BatchWrite",
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(total),
		DataSize:        float64(total*wl.ValueSize) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

func benchNoKVRead(t *testing.T, mode string) {
	t.Helper()
	fmt.Printf("\n=== NoKV Read (%s, sync=%v) ===\n", strings.ToUpper(mode), wl.SyncWrites)
	dir := engineDir("nokv", "read")
	db, err := openNoKVAt(dir, true)
	if err != nil {
		t.Fatalf("open nokv: %v", err)
	}

	preloadNoKV(t, db, wl.PreloadEntries, wl.BatchSize, wl.ValueSize, wl.Seed, "key-r-")

	if mode == "cold" {
		_ = db.Close()
		maybeDropCaches(t, "nokv-read")
		db, err = openNoKVAt(dir, false)
		if err != nil {
			t.Fatalf("reopen nokv: %v", err)
		}
	}
	defer db.Close()

	start := time.Now()
	total := wl.ReadOps
	W := concurrency()

	parallelDo(t, W, total, func(s, e int) error {
		for i := s; i < e; i++ {
			key := fmt.Appendf(nil, "key-r-%016d", i%wl.PreloadEntries)
			if err := db.View(func(txn *NoKV.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				if item == nil || item.Entry() == nil || len(item.Entry().Value) == 0 {
					return fmt.Errorf("missing value for key")
				}
				return nil
			}); err != nil {
				return fmt.Errorf("nokv read: %w", err)
			}
		}
		return nil
	})
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "NoKV Read",
		Engine:          "NoKV",
		Operation:       "Read",
		Mode:            mode,
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(total),
		DataSize:        float64(total*wl.ValueSize) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

func benchBadgerRead(t *testing.T, mode string) {
	t.Helper()
	fmt.Printf("\n=== Badger Read (%s, sync=%v) ===\n", strings.ToUpper(mode), wl.SyncWrites)
	dir := engineDir("badger", "read")
	db, err := openBadgerAt(dir, true)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	preloadBadger(t, db, wl.PreloadEntries, wl.BatchSize, wl.ValueSize, wl.Seed, "key-r-")

	if mode == "cold" {
		_ = db.Close()
		maybeDropCaches(t, "badger-read")
		db, err = openBadgerAt(dir, false)
		if err != nil {
			t.Fatalf("reopen badger: %v", err)
		}
	}
	defer db.Close()

	start := time.Now()
	total := wl.ReadOps
	W := concurrency()

	parallelDo(t, W, total, func(s, e int) error {
		for i := s; i < e; i++ {
			key := fmt.Appendf(nil, "key-r-%016d", i%wl.PreloadEntries)
			if err := db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				_, err = item.ValueCopy(nil)
				return err
			}); err != nil {
				return fmt.Errorf("badger read: %w", err)
			}
		}
		return nil
	})
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "Badger Read",
		Engine:          "Badger",
		Operation:       "Read",
		Mode:            mode,
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(total),
		DataSize:        float64(total*wl.ValueSize) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

func benchNoKVRange(t *testing.T, mode string) {
	t.Helper()
	fmt.Printf("\n=== NoKV RangeQuery (%s, window=%d) ===\n", strings.ToUpper(mode), wl.RangeWindow)
	dir := engineDir("nokv", "range")
	db, err := openNoKVAt(dir, true)
	if err != nil {
		t.Fatalf("open nokv: %v", err)
	}

	preloadNoKV(t, db, wl.PreloadEntries, wl.BatchSize, wl.ValueSize, wl.Seed, "key-g-")

	if mode == "cold" {
		_ = db.Close()
		maybeDropCaches(t, "nokv-range")
		db, err = openNoKVAt(dir, false)
		if err != nil {
			t.Fatalf("reopen nokv: %v", err)
		}
	}
	defer db.Close()

	start := time.Now()
	totalQ := wl.RangeQueries
	var totalItems int64

	W := concurrency()
	var mu sync.Mutex

	parallelDo(t, W, totalQ, func(s, e int) error {
		localCount := int64(0)
		for i := s; i < e; i++ {
			if err := db.View(func(txn *NoKV.Txn) error {
				it := txn.NewIterator(NoKV.IteratorOptions{
					Prefix:  []byte("key-g-"),
					KeyOnly: true,
				})
				defer it.Close()
				cnt := 0
				for it.Rewind(); it.Valid() && cnt < wl.RangeWindow; it.Next() {
					if it.Item() == nil {
						break
					}
					cnt++
				}
				localCount += int64(cnt)
				return nil
			}); err != nil {
				return fmt.Errorf("nokv range: %w", err)
			}
		}
		mu.Lock()
		totalItems += localCount
		mu.Unlock()
		return nil
	})
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "NoKV RangeQuery",
		Engine:          "NoKV",
		Operation:       "RangeQuery",
		Mode:            mode,
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(totalQ),
		DataSize:        float64(totalItems*int64(wl.ValueSize)) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

func benchBadgerRange(t *testing.T, mode string) {
	t.Helper()
	fmt.Printf("\n=== Badger RangeQuery (%s, window=%d) ===\n", strings.ToUpper(mode), wl.RangeWindow)
	dir := engineDir("badger", "range")
	db, err := openBadgerAt(dir, true)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	preloadBadger(t, db, wl.PreloadEntries, wl.BatchSize, wl.ValueSize, wl.Seed, "key-g-")

	if mode == "cold" {
		_ = db.Close()
		maybeDropCaches(t, "badger-range")
		db, err = openBadgerAt(dir, false)
		if err != nil {
			t.Fatalf("reopen badger: %v", err)
		}
	}
	defer db.Close()

	start := time.Now()
	totalQ := wl.RangeQueries
	var totalItems int64

	W := concurrency()
	var mu sync.Mutex

	parallelDo(t, W, totalQ, func(s, e int) error {
		local := int64(0)
		for i := s; i < e; i++ {
			if err := db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = []byte("key-g-")
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				cnt := 0
				for it.Rewind(); it.ValidForPrefix(opts.Prefix) && cnt < wl.RangeWindow; it.Next() {
					item := it.Item()
					if item == nil {
						break
					}
					cnt++
				}
				local += int64(cnt)
				return nil
			}); err != nil {
				return fmt.Errorf("badger range: %w", err)
			}
		}
		mu.Lock()
		totalItems += local
		mu.Unlock()
		return nil
	})
	dur := time.Since(start)

	res := BenchmarkResult{
		Name:            "Badger RangeQuery",
		Engine:          "Badger",
		Operation:       "RangeQuery",
		Mode:            mode,
		StartTime:       start,
		EndTime:         start.Add(dur),
		TotalDuration:   dur,
		TotalOperations: int64(totalQ),
		DataSize:        float64(totalItems*int64(wl.ValueSize)) / (1024 * 1024),
	}
	res.Finalize()
	allResults = append(allResults, res)
}

// ------------------------------ Reporting ------------------------------

func writeResultsJSONCSV(results []BenchmarkResult, baseDir string) error {
	jsPath := filepath.Join(baseDir, "benchmark_results.json")
	csPath := filepath.Join(baseDir, "benchmark_results.csv")

	jsFile, err := os.Create(jsPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(jsFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		_ = jsFile.Close()
		return err
	}
	_ = jsFile.Close()

	csFile, err := os.Create(csPath)
	if err != nil {
		return err
	}
	w := csv.NewWriter(csFile)
	defer csFile.Close()
	defer w.Flush()

	if err := w.Write([]string{"name", "engine", "operation", "mode", "ops_per_sec", "avg_latency_ns", "total_ops", "data_mb", "duration_ns", "start", "end"}); err != nil {
		return err
	}
	for _, r := range results {
		if err := w.Write([]string{
			r.Name,
			r.Engine,
			r.Operation,
			r.Mode,
			fmt.Sprintf("%.0f", r.Throughput),
			fmt.Sprintf("%.0f", r.AvgLatencyNS),
			fmt.Sprintf("%d", r.TotalOperations),
			fmt.Sprintf("%.2f", r.DataSize),
			fmt.Sprintf("%d", r.TotalDuration.Nanoseconds()),
			r.StartTime.Format(time.RFC3339),
			r.EndTime.Format(time.RFC3339),
		}); err != nil {
			return err
		}
	}
	return nil
}

// ------------------------------ Test Entry ------------------------------

func TestMain(m *testing.M) {
	flag.Parse()

	wl = workloadConfig{
		ValueSize:      *fValueSize,
		PreloadEntries: *fPreloadEntries,
		WriteOps:       *fWriteOps,
		ReadOps:        *fReadOps,
		BatchEntries:   *fBatchEntries,
		BatchSize:      *fBatchSize,
		RangeQueries:   *fRangeQueries,
		RangeWindow:    *fRangeWindow,
		Seed:           *fSeed,
		SyncWrites:     *fSyncWrites,
		Conc:           *fConc,
		Mode:           strings.ToLower(*fMode),
		Dir:            *fBenchDir,
		BadgerBlockMB:  *fBadgerBlockMB,
		BadgerIndexMB:  *fBadgerIndexMB,
		BadgerComp:     strings.ToLower(*fBadgerCompression),
		ValueThreshold: *fValueThreshold,
		DropCacheMode:  strings.ToLower(*fDropCacheMode),
		DropCacheWait:  *fDropCacheWait,
	}

	os.Exit(m.Run())
}

func TestBenchmarkResults(t *testing.T) {
	if os.Getenv(benchmarkEnvKey) != "1" {
		t.Skipf("set %s=1 to run benchmark suite", benchmarkEnvKey)
	}

	allResults = allResults[:0]

	fmt.Printf("\n=== Benchmark Suite @ %s ===\n", nowStr())
	fmt.Printf("Dir=%s seed=%d sync=%v conc=%d rounds=%d mode=%s\n", wl.Dir, wl.Seed, wl.SyncWrites, concurrency(), *fRounds, wl.Mode)
	fmt.Printf("ValueSize=%dB preload=%d writeOps=%d readOps=%d batchEntries=%d batchSize=%d rangeQ=%d window=%d\n",
		wl.ValueSize, wl.PreloadEntries, wl.WriteOps, wl.ReadOps, wl.BatchEntries, wl.BatchSize, wl.RangeQueries, wl.RangeWindow)
	fmt.Printf("Badger: block=%dMB index=%dMB comp=%s\n\n", wl.BadgerBlockMB, wl.BadgerIndexMB, wl.BadgerComp)

	for r := 1; r <= *fRounds; r++ {
		fmt.Printf("---- Round %d ----\n", r)

		benchNoKVWrite(t)
		benchBadgerWrite(t)
		benchNoKVBatchWrite(t)
		benchBadgerBatchWrite(t)

		switch wl.Mode {
		case "warm":
			benchNoKVRead(t, "warm")
			benchBadgerRead(t, "warm")
			benchNoKVRange(t, "warm")
			benchBadgerRange(t, "warm")
		case "cold":
			benchNoKVRead(t, "cold")
			benchBadgerRead(t, "cold")
			benchNoKVRange(t, "cold")
			benchBadgerRange(t, "cold")
		case "both":
			benchNoKVRead(t, "warm")
			benchBadgerRead(t, "warm")
			benchNoKVRange(t, "warm")
			benchBadgerRange(t, "warm")
			benchNoKVRead(t, "cold")
			benchBadgerRead(t, "cold")
			benchNoKVRange(t, "cold")
			benchBadgerRange(t, "cold")
		default:
			t.Fatalf("unknown mode: %s", wl.Mode)
		}
	}

	sort.SliceStable(allResults, func(i, j int) bool {
		ri, rj := allResults[i], allResults[j]
		key := func(r BenchmarkResult) string {
			return strings.Join([]string{r.Name, r.Engine, r.Operation, r.Mode, r.StartTime.Format(time.RFC3339Nano)}, "|")
		}
		return key(ri) < key(rj)
	})

	fmt.Printf("\nBenchmark Summary:\n")
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	writeSummaryTable(tw, allResults)
	fmt.Println()

	must(os.MkdirAll(wl.Dir, 0o755))
	if err := writeResultsJSONCSV(allResults, wl.Dir); err != nil {
		t.Fatalf("write results failed: %v", err)
	}
}
