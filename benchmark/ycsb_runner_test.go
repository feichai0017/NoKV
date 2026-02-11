package benchmark

import (
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fixedSource struct {
	val int64
}

func (s fixedSource) Int63() int64 { return s.val }
func (s *fixedSource) Seed(seed int64) {
	s.val = seed
}

func randWithFloat(p float64) *rand.Rand {
	if p < 0 {
		p = 0
	}
	if p >= 1 {
		p = math.Nextafter(1, 0)
	}
	val := int64(p * (1 << 63))
	if val < 0 {
		val = 0
	}
	return rand.New(&fixedSource{val: val})
}

type fakeEngine struct {
	mu      sync.Mutex
	data    map[string][]byte
	opens   int
	closes  int
	cleans  int
	reads   int
	inserts int
	updates int
	scans   int
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{data: make(map[string][]byte)}
}

func (e *fakeEngine) Name() string { return "fake" }
func (e *fakeEngine) Open(clean bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.opens++
	if clean {
		e.cleans++
		e.data = make(map[string][]byte)
	}
	return nil
}
func (e *fakeEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closes++
	return nil
}
func (e *fakeEngine) Read(key []byte, dst []byte) ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reads++
	if v, ok := e.data[string(key)]; ok {
		out := append(dst[:0], v...)
		return out, nil
	}
	return dst[:0], nil
}
func (e *fakeEngine) Insert(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.data == nil {
		e.data = make(map[string][]byte)
	}
	e.inserts++
	e.data[string(key)] = append([]byte(nil), value...)
	return nil
}
func (e *fakeEngine) Update(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.updates++
	e.data[string(key)] = append([]byte(nil), value...)
	return nil
}
func (e *fakeEngine) Scan(_ []byte, count int) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.scans++
	if count <= 0 {
		return 0, nil
	}
	if len(e.data) < count {
		return len(e.data), nil
	}
	return count, nil
}

type fixedGen struct {
	v int64
}

func (g fixedGen) Next(_ int64) int64 { return g.v }

func TestParsePercentilesAndValueSizer(t *testing.T) {
	pts, maxSz := parsePercentiles("50:128,90:256,100:512", 64)
	require.Len(t, pts, 3)
	require.Equal(t, 512, maxSz)

	pts, maxSz = parsePercentiles("bad,,10:-2,110:20,80:0", 32)
	require.Len(t, pts, 1)
	require.Equal(t, 32, maxSz)

	cfg := ycsbConfig{
		ValueSize: 16,
		ValueDist: "uniform",
		ValueMin:  8,
		ValueMax:  12,
	}
	sizer := newValueSizer(cfg)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5; i++ {
		val := sizer.Next(rng)
		require.GreaterOrEqual(t, val, 8)
		require.LessOrEqual(t, val, 12)
	}

	cfg.ValueDist = "normal"
	cfg.ValueMin = 1
	cfg.ValueMax = 3
	cfg.ValueStd = 1
	sizer = newValueSizer(cfg)
	for i := 0; i < 5; i++ {
		val := sizer.Next(rng)
		require.GreaterOrEqual(t, val, 1)
		require.LessOrEqual(t, val, 3)
	}

	cfg.ValueDist = "percentile"
	cfg.ValuePct = "50:100,90:200,100:300"
	sizer = newValueSizer(cfg)
	require.Equal(t, 100, sizer.Next(randWithFloat(0.2)))
	require.Equal(t, 200, sizer.Next(randWithFloat(0.7)))
	require.Equal(t, 300, sizer.Next(randWithFloat(0.99)))

	cfg.ValueDist = ""
	sizer = newValueSizer(cfg)
	require.Equal(t, cfg.ValueSize, sizer.Next(nil))
}

func TestWorkloadParsingAndHelpers(t *testing.T) {
	_, err := parseYCSBWorkloads("")
	require.Error(t, err)
	_, err = parseYCSBWorkloads("Z")
	require.Error(t, err)
	wls, err := parseYCSBWorkloads("A,B")
	require.NoError(t, err)
	require.Len(t, wls, 2)

	engines := parseYCSBEngines("nokv, badger ,, rocksdb")
	require.Equal(t, []string{"nokv", "badger", "rocksdb"}, engines)

	wl := ycsbWorkload{
		ReadRatio:       0.2,
		UpdateRatio:     0.2,
		InsertRatio:     0.2,
		ScanRatio:       0.2,
		ReadModifyWrite: 0.1,
	}
	require.Equal(t, "read", chooseOperation(randWithFloat(0.1), wl))
	require.Equal(t, "update", chooseOperation(randWithFloat(0.25), wl))
	require.Equal(t, "insert", chooseOperation(randWithFloat(0.45), wl))
	require.Equal(t, "scan", chooseOperation(randWithFloat(0.65), wl))
	require.Equal(t, "readmodifywrite", chooseOperation(randWithFloat(0.85), wl))
	require.Equal(t, "read", chooseOperation(randWithFloat(0.95), wl))

	state := &ycsbKeyspace{baseRecords: 3}
	require.Equal(t, int64(3), state.totalRecords())
	require.Equal(t, int64(3), state.nextInsertID())
	require.Equal(t, int64(4), state.nextInsertID())
	require.Equal(t, int64(5), state.totalRecords())

	key := selectExistingKey(fixedGen{v: 99}, state)
	require.Equal(t, "user000000000004", string(key))
	state = &ycsbKeyspace{}
	key = selectExistingKey(fixedGen{v: 0}, state)
	require.Equal(t, "user000000000000", string(key))

	require.Equal(t, "user000000000123", string(formatYCSBKey(123)))
}

func TestValuePoolAndRandomValue(t *testing.T) {
	buf := valueBufPool.Get(0)
	require.Nil(t, buf)

	got := valueBufPool.Get(8)
	require.Len(t, got, 8)
	valueBufPool.Put(got)

	out := randomValue(rand.New(rand.NewSource(1)), 4)
	require.Len(t, out, 4)
	valueBufPool.Put(out)
}

func TestGeneratorsAndRunYCSBErrors(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	zipf := newZipfGenerator(rng, 10)
	require.Equal(t, int64(0), zipf.Next(0))
	val := zipf.Next(5)
	require.GreaterOrEqual(t, val, int64(0))
	require.Less(t, val, int64(5))

	uni := &uniformGenerator{r: rand.New(rand.NewSource(2))}
	require.Equal(t, int64(0), uni.Next(0))
	uniVal := uni.Next(7)
	require.GreaterOrEqual(t, uniVal, int64(0))
	require.Less(t, uniVal, int64(7))

	state := &ycsbKeyspace{}
	latest := newLatestGenerator(rand.New(rand.NewSource(3)), 10, state)
	require.Equal(t, int64(0), latest.Next(0))
	state.baseRecords = 10
	latestVal := latest.Next(10)
	require.GreaterOrEqual(t, latestVal, int64(0))
	require.Less(t, latestVal, int64(10))

	_, err := runYCSBBenchmarks(ycsbConfig{Engines: []string{"unknown"}}, ycsbEngineOptions{})
	require.Error(t, err)
}

func TestWriteYCSBSummary(t *testing.T) {
	dir := t.TempDir()
	results := []BenchmarkResult{{
		Name:            "fake-YCSB-A",
		Engine:          "fake",
		Operation:       "YCSB-A",
		TotalOperations: 10,
		TotalDuration:   time.Millisecond,
	}}
	results[0].Finalize()

	require.NoError(t, writeYCSBSummary(results, dir))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	path := filepath.Join(dir, entries[0].Name())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "name,engine,workload,ops")
}

func TestYCSBLoadAndRunWorkload(t *testing.T) {
	engine := newFakeEngine()
	cfg := ycsbConfig{
		Seed:        1,
		RecordCount: 4,
		Operations:  3,
		ValueSize:   8,
		ValueDist:   "fixed",
		Concurrency: 1,
		ScanLength:  2,
		TargetOps:   1000,
	}
	state := &ycsbKeyspace{baseRecords: int64(cfg.RecordCount)}
	require.NoError(t, ycsbLoad(engine, cfg, state))
	require.Equal(t, 0, int(state.inserted.Load()))

	workloads := []ycsbWorkload{
		{Name: "R", ReadRatio: 1, Distribution: "uniform", Description: "read"},
		{Name: "U", UpdateRatio: 1, Distribution: "uniform", Description: "update"},
		{Name: "I", InsertRatio: 1, Distribution: "uniform", Description: "insert"},
		{Name: "S", ScanRatio: 1, Distribution: "uniform", Description: "scan"},
		{Name: "M", ReadModifyWrite: 1, Distribution: "uniform", Description: "rmw"},
	}
	for _, wl := range workloads {
		res, err := ycsbRunWorkload(engine, cfg, state, wl)
		require.NoError(t, err)
		require.Equal(t, int64(cfg.Operations), res.TotalOperations)
	}
}

func TestRunYCSBWorkloadsOnEngineIsolated(t *testing.T) {
	cfg := ycsbConfig{
		Seed:        1,
		RecordCount: 5,
		Operations:  3,
		ValueSize:   8,
		ValueDist:   "fixed",
		Concurrency: 1,
		ScanLength:  2,
		Workloads: []ycsbWorkload{
			{Name: "C1", ReadRatio: 1, Distribution: "uniform", Description: "read-1"},
			{Name: "C2", ReadRatio: 1, Distribution: "uniform", Description: "read-2"},
		},
	}
	engine := newFakeEngine()

	results, err := runYCSBWorkloadsOnEngine(engine, cfg)
	require.NoError(t, err)
	require.Len(t, results, 2)

	engine.mu.Lock()
	defer engine.mu.Unlock()
	require.Equal(t, 2, engine.opens)
	require.Equal(t, 2, engine.cleans)
	require.Equal(t, 2, engine.closes)
	require.Equal(t, cfg.RecordCount*len(cfg.Workloads), engine.inserts)
}

func TestKeyGenerators(t *testing.T) {
	state := &ycsbKeyspace{baseRecords: 10}
	rng := rand.New(rand.NewSource(1))

	gen := newKeyGenerator("latest", rng, 10, state)
	require.IsType(t, &latestGenerator{}, gen)
	require.GreaterOrEqual(t, gen.Next(10), int64(0))

	gen = newKeyGenerator("uniform", rng, 10, state)
	require.IsType(t, &uniformGenerator{}, gen)
	require.GreaterOrEqual(t, gen.Next(10), int64(0))

	gen = newKeyGenerator("zipfian", rng, 10, state)
	require.IsType(t, &zipfGenerator{}, gen)
}

func TestMinHelper(t *testing.T) {
	require.Equal(t, 1, min(1, 2))
	require.Equal(t, 2, min(3, 2))
}
