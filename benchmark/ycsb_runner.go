package benchmark

import (
	"encoding/csv"
	"expvar"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ycsbWorkload struct {
	Name            string
	ReadRatio       float64
	UpdateRatio     float64
	InsertRatio     float64
	ScanRatio       float64
	ReadModifyWrite float64
	Distribution    string
	Description     string
}

var defaultYCSBWorkloads = map[string]ycsbWorkload{
	"A": {Name: "A", ReadRatio: 0.5, UpdateRatio: 0.5, Distribution: "zipfian", Description: "50/50 read/update"},
	"B": {Name: "B", ReadRatio: 0.95, UpdateRatio: 0.05, Distribution: "zipfian", Description: "95/5 read/update"},
	"C": {Name: "C", ReadRatio: 1.0, Distribution: "zipfian", Description: "100% read"},
	"D": {Name: "D", ReadRatio: 0.95, InsertRatio: 0.05, Distribution: "latest", Description: "95% read, 5% insert (latest)"},
	"E": {Name: "E", ScanRatio: 0.95, InsertRatio: 0.05, Distribution: "zipfian", Description: "95% scan, 5% insert"},
	"F": {Name: "F", ReadRatio: 0.5, ReadModifyWrite: 0.5, Distribution: "zipfian", Description: "read-modify-write"},
}

type ycsbConfig struct {
	BaseDir     string
	Seed        int64
	RecordCount int
	Operations  int
	WarmUpOps   int
	ValueSize   int
	Concurrency int
	ScanLength  int
	Workloads   []ycsbWorkload
	Engines     []string
}

type ycsbKeyspace struct {
	baseRecords int64
	inserted    atomic.Int64
}

func (ks *ycsbKeyspace) totalRecords() int64 {
	return ks.baseRecords + ks.inserted.Load()
}

func (ks *ycsbKeyspace) nextInsertID() int64 {
	n := ks.inserted.Add(1)
	return ks.baseRecords + n - 1
}

type keyGenerator interface {
	Next(max int64) int64
}

type zipfGenerator struct {
	r    *rand.Rand
	zipf *rand.Zipf
}

func newZipfGenerator(r *rand.Rand, span uint64) *zipfGenerator {
	if span == 0 {
		span = 1
	}
	return &zipfGenerator{
		r:    r,
		zipf: rand.NewZipf(r, 1.01, 1, span),
	}
}

func (g *zipfGenerator) Next(max int64) int64 {
	if max <= 0 {
		return 0
	}
	for {
		v := int64(g.zipf.Uint64())
		if v < max {
			return v
		}
	}
}

type uniformGenerator struct {
	r *rand.Rand
}

func (g *uniformGenerator) Next(max int64) int64 {
	if max <= 0 {
		return 0
	}
	return g.r.Int63n(max)
}

type latestGenerator struct {
	state *ycsbKeyspace
	zipf  *rand.Zipf
}

func newLatestGenerator(r *rand.Rand, span uint64, state *ycsbKeyspace) *latestGenerator {
	if span == 0 {
		span = 1
	}
	return &latestGenerator{
		state: state,
		zipf:  rand.NewZipf(r, 1.01, 1, span),
	}
}

func (g *latestGenerator) Next(max int64) int64 {
	total := g.state.totalRecords()
	if total <= 0 {
		return 0
	}
	latest := total - 1
	offset := int64(g.zipf.Uint64())
	id := latest - offset
	if id < 0 {
		id = 0
	}
	if id >= max {
		return latest % max
	}
	return id
}

func runYCSBBenchmarks(cfg ycsbConfig, opts ycsbEngineOptions) ([]BenchmarkResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = runtime.GOMAXPROCS(0)
		if cfg.Concurrency <= 0 {
			cfg.Concurrency = 1
		}
	}
	results := make([]BenchmarkResult, 0)
	for _, engineName := range cfg.Engines {
		engineName = strings.ToLower(strings.TrimSpace(engineName))
		if engineName == "" {
			continue
		}
		var engine ycsbEngine
		switch engineName {
		case "nokv":
			engine = newNoKVEngine(opts)
		case "badger":
			engine = newBadgerEngine(opts)
		case "rocksdb":
			engine = newRocksDBEngine(opts)
		case "redis":
			engine = newRedisEngine(opts)
		default:
			return nil, fmt.Errorf("unknown engine %q", engineName)
		}
		if err := engine.Open(true); err != nil {
			return nil, fmt.Errorf("%s open: %w", engine.Name(), err)
		}
		state := &ycsbKeyspace{baseRecords: int64(cfg.RecordCount)}
		if err := ycsbLoad(engine, cfg, state); err != nil {
			_ = engine.Close()
			return nil, fmt.Errorf("%s load: %w", engine.Name(), err)
		}
		for _, workload := range cfg.Workloads {
			if cfg.WarmUpOps > 0 {
				warmCfg := cfg
				warmCfg.Operations = cfg.WarmUpOps
				warmCfg.WarmUpOps = 0
				if _, err := ycsbRunWorkload(engine, warmCfg, state, workload); err != nil {
					_ = engine.Close()
					return nil, fmt.Errorf("%s warmup %s: %w", engine.Name(), workload.Name, err)
				}
			}
			res, err := ycsbRunWorkload(engine, cfg, state, workload)
			if err != nil {
				_ = engine.Close()
				return nil, err
			}
			results = append(results, res)
		}
		if err := engine.Close(); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func ycsbLoad(engine ycsbEngine, cfg ycsbConfig, state *ycsbKeyspace) error {
	records := cfg.RecordCount
	perWorker := (records + cfg.Concurrency - 1) / cfg.Concurrency
	errCh := make(chan error, cfg.Concurrency)
	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Concurrency; worker++ {
		start := worker * perWorker
		end := min((worker+1)*perWorker, records)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(s)))
			value := make([]byte, cfg.ValueSize)
			for i := s; i < e; i++ {
				key := formatYCSBKey(int64(i))
				if _, err := rng.Read(value); err != nil {
					errCh <- err
					return
				}
				if err := engine.Insert(key, value); err != nil {
					errCh <- err
					return
				}
			}
		}(start, end)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	state.inserted.Store(0)
	return nil
}

func ycsbRunWorkload(engine ycsbEngine, cfg ycsbConfig, state *ycsbKeyspace, wl ycsbWorkload) (BenchmarkResult, error) {
	totalOps := cfg.Operations
	opsPerWorker := (totalOps + cfg.Concurrency - 1) / cfg.Concurrency
	latRec := newLatencyRecorder(totalOps)
	var loadedOps atomic.Int64
	var (
		readOps   atomic.Int64
		updateOps atomic.Int64
		insertOps atomic.Int64
		scanOps   atomic.Int64
		rmwOps    atomic.Int64
		scanItems atomic.Int64
		dataBytes atomic.Int64
	)
	start := time.Now()
	errCh := make(chan error, cfg.Concurrency)
	var wg sync.WaitGroup

	keySpan := uint64(cfg.RecordCount + cfg.Operations*2)

	for worker := 0; worker < cfg.Concurrency; worker++ {
		localStart := worker * opsPerWorker
		localEnd := min((worker+1)*opsPerWorker, totalOps)
		if localStart >= localEnd {
			continue
		}
		wg.Add(1)
		go func(idx int, s, e int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(idx) + 1))
			readGen := newKeyGenerator(wl.Distribution, rng, keySpan, state)
			for i := s; i < e; i++ {
				opType := chooseOperation(rng, wl)
				startOp := time.Now()
				switch opType {
				case "read":
					key := selectExistingKey(readGen, state)
					valBuf := valueBufPool.Get(cfg.ValueSize)
					if _, err := engine.Read(key, valBuf); err != nil {
						errCh <- fmt.Errorf("%s read: %w", engine.Name(), err)
						return
					}
					valueBufPool.Put(valBuf)
					readOps.Add(1)
					dataBytes.Add(int64(cfg.ValueSize))
				case "update":
					key := selectExistingKey(readGen, state)
					val := randomValue(rng, cfg.ValueSize)
					if err := engine.Update(key, val); err != nil {
						errCh <- fmt.Errorf("%s update: %w", engine.Name(), err)
						return
					}
					valueBufPool.Put(val)
					updateOps.Add(1)
					dataBytes.Add(int64(cfg.ValueSize))
				case "insert":
					id := state.nextInsertID()
					key := formatYCSBKey(id)
					val := randomValue(rng, cfg.ValueSize)
					if err := engine.Insert(key, val); err != nil {
						errCh <- fmt.Errorf("%s insert: %w", engine.Name(), err)
						return
					}
					valueBufPool.Put(val)
					insertOps.Add(1)
					dataBytes.Add(int64(cfg.ValueSize))
				case "scan":
					key := selectExistingKey(readGen, state)
					items, err := engine.Scan(key, cfg.ScanLength)
					if err != nil {
						errCh <- fmt.Errorf("%s scan: %w", engine.Name(), err)
						return
					}
					scanOps.Add(1)
					if items > 0 {
						scanItems.Add(int64(items))
						dataBytes.Add(int64(items) * int64(cfg.ValueSize))
					}
				case "readmodifywrite":
					key := selectExistingKey(readGen, state)
					valBuf := valueBufPool.Get(cfg.ValueSize)
					if _, err := engine.Read(key, valBuf); err != nil {
						errCh <- fmt.Errorf("%s read: %w", engine.Name(), err)
						return
					}
					val := randomValue(rng, cfg.ValueSize)
					if err := engine.Update(key, val); err != nil {
						errCh <- fmt.Errorf("%s update: %w", engine.Name(), err)
						return
					}
					valueBufPool.Put(valBuf)
					valueBufPool.Put(val)
					rmwOps.Add(1)
					dataBytes.Add(int64(cfg.ValueSize) * 2)
				default:
					errCh <- fmt.Errorf("unknown op %s", opType)
					return
				}
				latRec.Record(time.Since(startOp).Nanoseconds())
				loadedOps.Add(1)
			}
		}(worker, localStart, localEnd)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return BenchmarkResult{}, err
		}
	}
	duration := time.Since(start)

	result := BenchmarkResult{
		Name:               fmt.Sprintf("%s-YCSB-%s", engine.Name(), wl.Name),
		Engine:             engine.Name(),
		Operation:          fmt.Sprintf("YCSB-%s", wl.Name),
		Mode:               wl.Description,
		StartTime:          start,
		EndTime:            start.Add(duration),
		TotalDuration:      duration,
		TotalOperations:    loadedOps.Load(),
		DataBytes:          dataBytes.Load(),
		ReadOps:            readOps.Load(),
		UpdateOps:          updateOps.Load(),
		InsertOps:          insertOps.Load(),
		ScanOps:            scanOps.Load(),
		ReadModifyWriteOps: rmwOps.Load(),
		ScanItems:          scanItems.Load(),
	}
	result.Finalize()
	result.P50LatencyNS = latRec.Percentile(50)
	result.P95LatencyNS = latRec.Percentile(95)
	result.P99LatencyNS = latRec.Percentile(99)
	return result, nil
}

func newKeyGenerator(dist string, rng *rand.Rand, span uint64, state *ycsbKeyspace) keyGenerator {
	switch strings.ToLower(dist) {
	case "latest":
		return newLatestGenerator(rng, span, state)
	case "uniform":
		return &uniformGenerator{r: rng}
	default:
		return newZipfGenerator(rng, span)
	}
}

func chooseOperation(r *rand.Rand, wl ycsbWorkload) string {
	p := r.Float64()
	switch {
	case p < wl.ReadRatio:
		return "read"
	case p < wl.ReadRatio+wl.UpdateRatio:
		return "update"
	case p < wl.ReadRatio+wl.UpdateRatio+wl.InsertRatio:
		return "insert"
	case p < wl.ReadRatio+wl.UpdateRatio+wl.InsertRatio+wl.ScanRatio:
		return "scan"
	case p < wl.ReadRatio+wl.UpdateRatio+wl.InsertRatio+wl.ScanRatio+wl.ReadModifyWrite:
		return "readmodifywrite"
	default:
		return "read"
	}
}

func selectExistingKey(gen keyGenerator, state *ycsbKeyspace) []byte {
	count := state.totalRecords()
	if count <= 0 {
		return formatYCSBKey(0)
	}
	id := gen.Next(count)
	if id >= count {
		id = count - 1
	}
	return formatYCSBKey(id)
}

func formatYCSBKey(id int64) []byte {
	buf := make([]byte, 0, 16)
	buf = append(buf, 'u', 's', 'e', 'r')
	// Zero-pad to 12 digits to retain fixed-width keys.
	var tmp [12]byte
	pos := len(tmp)
	if id == 0 {
		pos--
		tmp[pos] = '0'
	}
	for v := id; v > 0 && pos > 0; v /= 10 {
		pos--
		tmp[pos] = byte('0' + v%10)
	}
	for pos > 0 {
		pos--
		tmp[pos] = '0'
	}
	buf = append(buf, tmp[:]...)
	return buf
}

var (
	valueBufPool = &valuePool{
		pool:     sync.Pool{},
		gets:     expvar.NewInt("NoKV.Benchmark.ValuePool.Gets"),
		releases: expvar.NewInt("NoKV.Benchmark.ValuePool.Releases"),
	}
)

type valuePool struct {
	pool     sync.Pool
	gets     *expvar.Int
	releases *expvar.Int
}

func (vp *valuePool) Get(size int) []byte {
	vp.gets.Add(1)
	if size <= 0 {
		return nil
	}
	if v := vp.pool.Get(); v != nil {
		if buf, ok := v.([]byte); ok && cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size)
}

func (vp *valuePool) Put(buf []byte) {
	if buf == nil {
		return
	}
	vp.releases.Add(1)
	vp.pool.Put(buf)
}

func randomValue(rng *rand.Rand, size int) []byte {
	buf := valueBufPool.Get(size)
	if size <= 0 {
		return buf
	}
	if _, err := rng.Read(buf); err != nil {
		for i := range buf {
			buf[i] = byte(rng.Intn(256))
		}
	}
	return buf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeYCSBSummary(results []BenchmarkResult, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file := filepath.Join(dir, fmt.Sprintf("ycsb_results_%s.csv", time.Now().Format("20060102_150405")))
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"name", "engine", "workload", "ops", "ops_per_sec", "avg_ns", "p50_ns", "p95_ns", "p99_ns", "duration_ns", "data_bytes", "data_mb", "reads", "updates", "inserts", "scans", "scan_items", "rmw"}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, r := range results {
		row := []string{
			r.Name,
			r.Engine,
			r.Operation,
			fmt.Sprintf("%d", r.TotalOperations),
			fmt.Sprintf("%.0f", r.Throughput),
			fmt.Sprintf("%.0f", r.AvgLatencyNS),
			fmt.Sprintf("%.0f", r.P50LatencyNS),
			fmt.Sprintf("%.0f", r.P95LatencyNS),
			fmt.Sprintf("%.0f", r.P99LatencyNS),
			fmt.Sprintf("%d", r.TotalDuration.Nanoseconds()),
			fmt.Sprintf("%d", r.DataBytes),
			fmt.Sprintf("%.2f", r.DataSize),
			fmt.Sprintf("%d", r.ReadOps),
			fmt.Sprintf("%d", r.UpdateOps),
			fmt.Sprintf("%d", r.InsertOps),
			fmt.Sprintf("%d", r.ScanOps),
			fmt.Sprintf("%d", r.ScanItems),
			fmt.Sprintf("%d", r.ReadModifyWriteOps),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func parseYCSBWorkloads(spec string) ([]ycsbWorkload, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("empty workload list")
	}
	parts := strings.Split(spec, ",")
	workloads := make([]ycsbWorkload, 0, len(parts))
	for _, part := range parts {
		name := strings.ToUpper(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		wl, ok := defaultYCSBWorkloads[name]
		if !ok {
			return nil, fmt.Errorf("unknown workload %q", name)
		}
		workloads = append(workloads, wl)
	}
	if len(workloads) == 0 {
		return nil, fmt.Errorf("no workloads selected")
	}
	return workloads, nil
}

func parseYCSBEngines(spec string) []string {
	parts := strings.Split(spec, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
