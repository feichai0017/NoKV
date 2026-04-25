package workload

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
)

const (
	CheckpointStorm = "checkpoint-storm"
	HotspotFanIn    = "hotspot-fanin"
)

var ErrWorkloadFailed = errors.New("benchmark/fsmeta/workload: workload completed with operation errors")

// Client is the fsmeta operation surface needed by metadata workloads.
// fsmeta/client.GRPCClient satisfies this interface.
type Client interface {
	Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error
	ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error)
	ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
}

type CheckpointStormConfig struct {
	Mount             fsmeta.MountID
	RunID             string
	Clients           int
	Directories       int
	FilesPerDirectory int
	StartInode        fsmeta.InodeID
}

type HotspotFanInConfig struct {
	Mount          fsmeta.MountID
	RunID          string
	Clients        int
	Files          int
	ReadsPerClient int
	PageLimit      uint32
	ReadDirPlus    bool
	StartInode     fsmeta.InodeID
}

type Result struct {
	Name      string
	RunID     string
	StartedAt time.Time
	Duration  time.Duration
	Ops       int
	Errors    int
	Samples   []Sample
}

type Sample struct {
	Operation string
	Duration  time.Duration
	Error     string
}

type SummaryRow struct {
	Workload     string
	RunID        string
	Operation    string
	Count        int
	Errors       int
	Throughput   float64
	AverageUS    float64
	P50US        float64
	P95US        float64
	P99US        float64
	DurationSecs float64
}

func RunCheckpointStorm(ctx context.Context, cli Client, cfg CheckpointStormConfig) (Result, error) {
	cfg = normalizeCheckpointStormConfig(cfg)
	started := time.Now()
	rec := newRecorder()

	for i := 0; i < cfg.Directories; i++ {
		inode := cfg.StartInode + fsmeta.InodeID(i)
		name := fmt.Sprintf("storm-%s-dir-%04d", cfg.RunID, i)
		rec.recordCall("mkdir", func() error {
			return cli.Create(ctx, fsmeta.CreateRequest{
				Mount:  cfg.Mount,
				Parent: fsmeta.RootInode,
				Name:   name,
				Inode:  inode,
			}, fsmeta.InodeRecord{
				Type:      fsmeta.InodeTypeDirectory,
				Mode:      0o755,
				LinkCount: 1,
			})
		})
	}

	totalFiles := cfg.Directories * cfg.FilesPerDirectory
	var next atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Clients; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				idx := int(next.Add(1)) - 1
				if idx >= totalFiles {
					return
				}
				dir := idx % cfg.Directories
				file := idx / cfg.Directories
				parent := cfg.StartInode + fsmeta.InodeID(dir)
				inode := cfg.StartInode + fsmeta.InodeID(cfg.Directories+idx)
				name := fmt.Sprintf("storm-%s-dir-%04d-file-%08d", cfg.RunID, dir, file)
				rec.recordCall("create_checkpoint", func() error {
					return cli.Create(ctx, fsmeta.CreateRequest{
						Mount:  cfg.Mount,
						Parent: parent,
						Name:   name,
						Inode:  inode,
					}, fsmeta.InodeRecord{
						Type:      fsmeta.InodeTypeFile,
						Mode:      0o644,
						LinkCount: 1,
					})
				})
			}
		}(worker)
	}
	wg.Wait()

	return finishResult(CheckpointStorm, cfg.RunID, started, rec.snapshot())
}

func RunHotspotFanIn(ctx context.Context, cli Client, cfg HotspotFanInConfig) (Result, error) {
	cfg = normalizeHotspotFanInConfig(cfg)
	started := time.Now()
	rec := newRecorder()

	dirInode := cfg.StartInode
	dirName := fmt.Sprintf("hotspot-%s", cfg.RunID)
	rec.recordCall("mkdir", func() error {
		return cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  cfg.Mount,
			Parent: fsmeta.RootInode,
			Name:   dirName,
			Inode:  dirInode,
		}, fsmeta.InodeRecord{
			Type:      fsmeta.InodeTypeDirectory,
			Mode:      0o755,
			LinkCount: 1,
		})
	})
	for i := 0; i < cfg.Files; i++ {
		inode := cfg.StartInode + fsmeta.InodeID(i+1)
		name := fmt.Sprintf("hotspot-%s-file-%08d", cfg.RunID, i)
		rec.recordCall("seed_create", func() error {
			return cli.Create(ctx, fsmeta.CreateRequest{
				Mount:  cfg.Mount,
				Parent: dirInode,
				Name:   name,
				Inode:  inode,
			}, fsmeta.InodeRecord{
				Type:      fsmeta.InodeTypeFile,
				Mode:      0o644,
				LinkCount: 1,
			})
		})
	}

	op := "readdir"
	if cfg.ReadDirPlus {
		op = "readdirplus"
	}
	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Clients; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := fsmeta.ReadDirRequest{
				Mount:  cfg.Mount,
				Parent: dirInode,
				Limit:  cfg.PageLimit,
			}
			for i := 0; i < cfg.ReadsPerClient; i++ {
				if cfg.ReadDirPlus {
					rec.recordCall(op, func() error {
						_, err := cli.ReadDirPlus(ctx, req)
						return err
					})
				} else {
					rec.recordCall(op, func() error {
						_, err := cli.ReadDir(ctx, req)
						return err
					})
				}
			}
		}()
	}
	wg.Wait()

	return finishResult(HotspotFanIn, cfg.RunID, started, rec.snapshot())
}

func SummaryRows(result Result) []SummaryRow {
	byOp := make(map[string][]Sample)
	for _, sample := range result.Samples {
		byOp[sample.Operation] = append(byOp[sample.Operation], sample)
	}
	ops := make([]string, 0, len(byOp))
	for op := range byOp {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	rows := make([]SummaryRow, 0, len(ops))
	for _, op := range ops {
		samples := byOp[op]
		var total time.Duration
		var errors int
		latencies := make([]time.Duration, 0, len(samples))
		for _, sample := range samples {
			total += sample.Duration
			latencies = append(latencies, sample.Duration)
			if sample.Error != "" {
				errors++
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		count := len(samples)
		avgUS := 0.0
		if count > 0 {
			avgUS = float64(total.Microseconds()) / float64(count)
		}
		rows = append(rows, SummaryRow{
			Workload:     result.Name,
			RunID:        result.RunID,
			Operation:    op,
			Count:        count,
			Errors:       errors,
			Throughput:   throughput(count, result.Duration),
			AverageUS:    avgUS,
			P50US:        percentileUS(latencies, 0.50),
			P95US:        percentileUS(latencies, 0.95),
			P99US:        percentileUS(latencies, 0.99),
			DurationSecs: result.Duration.Seconds(),
		})
	}
	return rows
}

func WriteSummaryCSV(w io.Writer, rows []SummaryRow) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{
		"workload",
		"run_id",
		"operation",
		"count",
		"errors",
		"throughput_ops_sec",
		"avg_latency_us",
		"p50_latency_us",
		"p95_latency_us",
		"p99_latency_us",
		"duration_sec",
	}); err != nil {
		return err
	}
	for _, row := range rows {
		if err := cw.Write([]string{
			row.Workload,
			row.RunID,
			row.Operation,
			strconv.Itoa(row.Count),
			strconv.Itoa(row.Errors),
			formatFloat(row.Throughput),
			formatFloat(row.AverageUS),
			formatFloat(row.P50US),
			formatFloat(row.P95US),
			formatFloat(row.P99US),
			formatFloat(row.DurationSecs),
		}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func NewRunID() string {
	return time.Now().UTC().Format("20060102T150405.000000000")
}

type recorder struct {
	mu      sync.Mutex
	samples []Sample
}

func newRecorder() *recorder {
	return &recorder{}
}

func (r *recorder) record(operation string, duration time.Duration, err error) {
	sample := Sample{Operation: operation, Duration: duration}
	if err != nil {
		sample.Error = err.Error()
	}
	r.mu.Lock()
	r.samples = append(r.samples, sample)
	r.mu.Unlock()
}

func (r *recorder) recordCall(operation string, fn func() error) {
	duration, err := timeCall(fn)
	r.record(operation, duration, err)
}

func (r *recorder) snapshot() []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Sample, len(r.samples))
	copy(out, r.samples)
	return out
}

func timeCall(fn func() error) (time.Duration, error) {
	start := time.Now()
	err := fn()
	return time.Since(start), err
}

func finishResult(name, runID string, started time.Time, samples []Sample) (Result, error) {
	result := Result{
		Name:      name,
		RunID:     runID,
		StartedAt: started,
		Duration:  time.Since(started),
		Ops:       len(samples),
		Samples:   samples,
	}
	for _, sample := range samples {
		if sample.Error != "" {
			result.Errors++
		}
	}
	if result.Errors > 0 {
		return result, fmt.Errorf("%w: %d/%d operations failed", ErrWorkloadFailed, result.Errors, result.Ops)
	}
	return result, nil
}

func normalizeCheckpointStormConfig(cfg CheckpointStormConfig) CheckpointStormConfig {
	if cfg.Mount == "" {
		cfg.Mount = "fsmeta-workload"
	}
	if cfg.RunID == "" {
		cfg.RunID = NewRunID()
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 4
	}
	if cfg.Directories <= 0 {
		cfg.Directories = 8
	}
	if cfg.FilesPerDirectory <= 0 {
		cfg.FilesPerDirectory = 128
	}
	if cfg.StartInode == 0 {
		cfg.StartInode = 1_000_000
	}
	return cfg
}

func normalizeHotspotFanInConfig(cfg HotspotFanInConfig) HotspotFanInConfig {
	if cfg.Mount == "" {
		cfg.Mount = "fsmeta-workload"
	}
	if cfg.RunID == "" {
		cfg.RunID = NewRunID()
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 4
	}
	if cfg.Files <= 0 {
		cfg.Files = 1024
	}
	if cfg.ReadsPerClient <= 0 {
		cfg.ReadsPerClient = 64
	}
	if cfg.PageLimit == 0 {
		cfg.PageLimit = uint32(cfg.Files)
	}
	if cfg.PageLimit > fsmeta.MaxReadDirLimit {
		cfg.PageLimit = fsmeta.MaxReadDirLimit
	}
	if cfg.StartInode == 0 {
		cfg.StartInode = 2_000_000
	}
	return cfg
}

func percentileUS(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return float64(sorted[0].Microseconds())
	}
	if p >= 1 {
		return float64(sorted[len(sorted)-1].Microseconds())
	}
	idx := int(float64(len(sorted)-1) * p)
	return float64(sorted[idx].Microseconds())
}

func throughput(count int, duration time.Duration) float64 {
	if count == 0 || duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 3, 64)
}
