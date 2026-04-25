package workload

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
)

const (
	CheckpointStorm = "checkpoint-storm"
	HotspotFanIn    = "hotspot-fanin"
	WatchSubtree    = "watch-subtree"

	DriverNativeFSMetadata = "native-fsmeta"
	DriverGenericKV        = "generic-kv"
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

type WatchSubtreeConfig struct {
	Mount              fsmeta.MountID
	RunID              string
	Clients            int
	Files              int
	StartInode         fsmeta.InodeID
	BackPressureWindow uint32
}

type Result struct {
	Name      string
	Driver    string
	RunID     string
	StartedAt time.Time
	Duration  time.Duration
	Ops       int
	Errors    int
	Samples   []Sample
}

type WatchClient interface {
	Client
	WatchSubtree(ctx context.Context, req fsmeta.WatchRequest) (fsmetaclient.WatchSubscription, error)
}

type Sample struct {
	Operation string
	Duration  time.Duration
	Error     string
}

type SummaryRow struct {
	Workload     string
	Driver       string
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

func RunWatchSubtree(ctx context.Context, cli Client, cfg WatchSubtreeConfig) (Result, error) {
	watchCli, ok := cli.(WatchClient)
	if !ok {
		return Result{}, fmt.Errorf("watch-subtree requires native fsmeta watch client")
	}
	cfg = normalizeWatchSubtreeConfig(cfg)
	started := time.Now()
	rec := newRecorder()

	dirInode := cfg.StartInode
	dirName := fmt.Sprintf("watch-%s", cfg.RunID)
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
	prefix, err := fsmeta.EncodeDentryPrefix(cfg.Mount, dirInode)
	if err != nil {
		return Result{}, err
	}
	stream, err := watchCli.WatchSubtree(ctx, fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: cfg.BackPressureWindow,
	})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = stream.Close() }()

	warmupKey, err := fsmeta.EncodeDentryKey(cfg.Mount, dirInode, "watch-warmup")
	if err != nil {
		return Result{}, err
	}
	if err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  cfg.Mount,
		Parent: dirInode,
		Name:   "watch-warmup",
		Inode:  cfg.StartInode + fsmeta.InodeID(cfg.Files+1),
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, Mode: 0o644, LinkCount: 1}); err != nil {
		return Result{}, err
	}
	if err := waitForWatchKey(ctx, stream, warmupKey); err != nil {
		return Result{}, err
	}

	starts := newWatchStarts()
	done := make(chan error, 1)
	go collectWatchEvents(ctx, stream, starts, cfg.Files, rec, done)

	var next atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Clients; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(next.Add(1)) - 1
				if idx >= cfg.Files {
					return
				}
				inode := cfg.StartInode + fsmeta.InodeID(idx+1)
				name := fmt.Sprintf("watch-%s-file-%08d", cfg.RunID, idx)
				key, err := fsmeta.EncodeDentryKey(cfg.Mount, dirInode, name)
				if err != nil {
					rec.record("watch_create", 0, err)
					continue
				}
				starts.put(key, time.Now())
				rec.recordCall("watch_create", func() error {
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
		}()
	}
	wg.Wait()
	if err := <-done; err != nil {
		rec.record("watch_notify", 0, err)
	}

	return finishResult(WatchSubtree, cfg.RunID, started, rec.snapshot())
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
			Driver:       result.Driver,
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
		"driver",
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
			row.Driver,
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
		return result, fmt.Errorf("%w: %d/%d operations failed; samples: %s", ErrWorkloadFailed, result.Errors, result.Ops, firstErrorSummary(samples, 3))
	}
	return result, nil
}

func firstErrorSummary(samples []Sample, limit int) string {
	if limit <= 0 {
		limit = 1
	}
	out := make([]string, 0, limit)
	for _, sample := range samples {
		if sample.Error == "" {
			continue
		}
		out = append(out, fmt.Sprintf("%s: %s", sample.Operation, sample.Error))
		if len(out) >= limit {
			break
		}
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, "; ")
}

type watchStarts struct {
	mu     sync.Mutex
	values map[string]time.Time
}

func newWatchStarts() *watchStarts {
	return &watchStarts{values: make(map[string]time.Time)}
}

func (s *watchStarts) put(key []byte, started time.Time) {
	s.mu.Lock()
	s.values[string(key)] = started
	s.mu.Unlock()
}

func (s *watchStarts) pop(key []byte) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	started, ok := s.values[string(key)]
	if ok {
		delete(s.values, string(key))
	}
	return started, ok
}

func collectWatchEvents(ctx context.Context, stream fsmetaclient.WatchSubscription, starts *watchStarts, target int, rec *recorder, done chan<- error) {
	received := 0
	for received < target {
		evt, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			done <- err
			return
		}
		_ = stream.Ack(evt.Cursor)
		if started, ok := starts.pop(evt.Key); ok {
			rec.record("watch_notify", time.Since(started), nil)
			received++
		}
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		default:
		}
	}
	done <- nil
}

func waitForWatchKey(ctx context.Context, stream fsmetaclient.WatchSubscription, key []byte) error {
	for {
		evt, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = stream.Ack(evt.Cursor)
		if string(evt.Key) == string(key) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
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

func normalizeWatchSubtreeConfig(cfg WatchSubtreeConfig) WatchSubtreeConfig {
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
	if cfg.StartInode == 0 {
		cfg.StartInode = 3_000_000
	}
	if cfg.BackPressureWindow == 0 {
		cfg.BackPressureWindow = uint32(cfg.Files + 1)
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
