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

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	CheckpointStorm         = "checkpoint-storm"
	DurableSnapshot         = "durable-snapshot"
	HotspotFanIn            = "hotspot-fanin"
	WatchSubtree            = "watch-subtree"
	NegativeLookup          = "negative-lookup"
	MultiWorkspaceAutoscale = "multi-workspace-autoscale"

	DriverNativeFSMetadata = "native-fsmeta"
)

var ErrWorkloadFailed = errors.New("benchmark/fsmeta/workload: workload completed with operation errors")

const (
	maxOperationAttempts = 4
	operationRetryBase   = 10 * time.Millisecond
	operationRetryMax    = 100 * time.Millisecond
	watchDeliveryPoll    = 10 * time.Millisecond
	watchTailTimeout     = 10 * time.Second
)

// Client is the fsmeta operation surface needed by metadata workloads.
// fsmeta/client.GRPCClient satisfies this interface.
type Client interface {
	Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error)
	Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error)
	ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error)
	ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
}

type CheckpointStormConfig struct {
	Mount             fsmeta.MountID
	RunID             string
	Clients           int
	Directories       int
	FilesPerDirectory int
}

type HotspotFanInConfig struct {
	Mount          fsmeta.MountID
	RunID          string
	Clients        int
	Files          int
	ReadsPerClient int
	PageLimit      uint32
	ReadDirPlus    bool
}

type WatchSubtreeConfig struct {
	Mount              fsmeta.MountID
	RunID              string
	Clients            int
	Files              int
	BackPressureWindow uint32
}

type DurableSnapshotConfig struct {
	Mount     fsmeta.MountID
	RunID     string
	Files     int
	Snapshots int
	PageLimit uint32
}

type NegativeLookupConfig struct {
	Mount          fsmeta.MountID
	RunID          string
	Clients        int
	Keys           int
	ReadsPerClient int
	Parent         fsmeta.InodeID
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

type DurableSnapshotClient interface {
	Client
	SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error)
	RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error
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
	dirInodes := make([]fsmeta.InodeID, cfg.Directories)

	for i := 0; i < cfg.Directories; i++ {
		name := fmt.Sprintf("storm-%s-dir-%04d", cfg.RunID, i)
		rec.recordCall("mkdir", func() error {
			result, err := cli.Create(ctx, fsmeta.CreateRequest{
				Mount:  cfg.Mount,
				Parent: fsmeta.RootInode,
				Name:   name,
				Attrs: fsmeta.CreateAttrs{
					Type: fsmeta.InodeTypeDirectory,
					Mode: 0o755,
				},
			})
			if err == nil {
				dirInodes[i] = result.Inode.Inode
			}
			return err
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
				parent := dirInodes[dir]
				name := fmt.Sprintf("storm-%s-dir-%04d-file-%08d", cfg.RunID, dir, file)
				rec.recordCall("create_checkpoint", func() error {
					_, err := cli.Create(ctx, fsmeta.CreateRequest{
						Mount:  cfg.Mount,
						Parent: parent,
						Name:   name,
						Attrs: fsmeta.CreateAttrs{
							Type: fsmeta.InodeTypeFile,
							Mode: 0o644,
						},
					})
					return err
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

	dirName := fmt.Sprintf("hotspot-%s", cfg.RunID)
	var dirInode fsmeta.InodeID
	rec.recordCall("mkdir", func() error {
		result, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  cfg.Mount,
			Parent: fsmeta.RootInode,
			Name:   dirName,
			Attrs: fsmeta.CreateAttrs{
				Type: fsmeta.InodeTypeDirectory,
				Mode: 0o755,
			},
		})
		if err == nil {
			dirInode = result.Inode.Inode
		}
		return err
	})
	for i := 0; i < cfg.Files; i++ {
		name := fmt.Sprintf("hotspot-%s-file-%08d", cfg.RunID, i)
		rec.recordCall("seed_create", func() error {
			_, err := cli.Create(ctx, fsmeta.CreateRequest{
				Mount:  cfg.Mount,
				Parent: dirInode,
				Name:   name,
				Attrs: fsmeta.CreateAttrs{
					Type: fsmeta.InodeTypeFile,
					Mode: 0o644,
				},
			})
			return err
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

	dirName := fmt.Sprintf("watch-%s", cfg.RunID)
	var dirInode fsmeta.InodeID
	rec.recordCall("mkdir", func() error {
		result, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  cfg.Mount,
			Parent: fsmeta.RootInode,
			Name:   dirName,
			Attrs: fsmeta.CreateAttrs{
				Type: fsmeta.InodeTypeDirectory,
				Mode: 0o755,
			},
		})
		if err == nil {
			dirInode = result.Inode.Inode
		}
		return err
	})
	stream, err := watchCli.WatchSubtree(ctx, fsmeta.WatchRequest{
		Mount:              cfg.Mount,
		RootInode:          dirInode,
		BackPressureWindow: cfg.BackPressureWindow,
	})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = stream.Close() }()

	if _, err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  cfg.Mount,
		Parent: dirInode,
		Name:   "watch-warmup",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	}); err != nil {
		return Result{}, err
	}
	if err := waitForWatchName(ctx, stream, "watch-warmup"); err != nil {
		return Result{}, err
	}

	starts := newWatchStarts()
	var successfulCreates atomic.Int64
	var deliveredCreates atomic.Int64
	createsDone := make(chan struct{})
	done := make(chan error, 1)
	go collectWatchEvents(ctx, stream, starts, &successfulCreates, &deliveredCreates, createsDone, rec, done)

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
				name := fmt.Sprintf("watch-%s-file-%08d", cfg.RunID, idx)
				starts.put(name, time.Now())
				duration, err := timeCall(func() error {
					_, err := cli.Create(ctx, fsmeta.CreateRequest{
						Mount:  cfg.Mount,
						Parent: dirInode,
						Name:   name,
						Attrs: fsmeta.CreateAttrs{
							Type: fsmeta.InodeTypeFile,
							Mode: 0o644,
						},
					})
					return err
				})
				if err != nil {
					starts.delete(name)
				} else {
					successfulCreates.Add(1)
				}
				rec.record("watch_create", duration, err)
			}
		}()
	}
	wg.Wait()
	close(createsDone)
	closeWatchAfterDelivery(stream, &successfulCreates, &deliveredCreates)
	if watchErr := <-done; watchErr != nil {
		rec.record("watch_notify", 0, watchErr)
	}

	return finishResult(WatchSubtree, cfg.RunID, started, rec.snapshot())
}

func RunNegativeLookup(ctx context.Context, cli Client, cfg NegativeLookupConfig) (Result, error) {
	cfg = normalizeNegativeLookupConfig(cfg)
	started := time.Now()
	rec := newRecorder()

	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Clients; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < cfg.ReadsPerClient; i++ {
				idx := (worker*cfg.ReadsPerClient + i) % cfg.Keys
				name := fmt.Sprintf("missing-%s-%08d", cfg.RunID, idx)
				rec.recordCall("lookup_missing", func() error {
					_, err := cli.Lookup(ctx, fsmeta.LookupRequest{
						Mount:  cfg.Mount,
						Parent: cfg.Parent,
						Name:   name,
					})
					if errors.Is(err, fsmeta.ErrNotFound) {
						return nil
					}
					if err == nil {
						return fmt.Errorf("negative lookup unexpectedly found %q", name)
					}
					return err
				})
			}
		}(worker)
	}
	wg.Wait()

	return finishResult(NegativeLookup, cfg.RunID, started, rec.snapshot())
}

func RunDurableSnapshot(ctx context.Context, cli Client, cfg DurableSnapshotConfig) (Result, error) {
	snapshotCli, ok := cli.(DurableSnapshotClient)
	if !ok {
		return Result{}, fmt.Errorf("durable-snapshot requires native fsmeta snapshot client")
	}
	cfg = normalizeDurableSnapshotConfig(cfg)
	started := time.Now()
	rec := newRecorder()

	dirName := fmt.Sprintf("durable-snapshot-%s", cfg.RunID)
	var root fsmeta.InodeID
	rec.recordCall("mkdir", func() error {
		result, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  cfg.Mount,
			Parent: fsmeta.RootInode,
			Name:   dirName,
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory, Mode: 0o755},
		})
		if err == nil {
			root = result.Inode.Inode
		}
		return err
	})
	for i := 0; i < cfg.Files; i++ {
		name := fmt.Sprintf("snapshot-%s-file-%08d", cfg.RunID, i)
		rec.recordCall("seed_create", func() error {
			_, err := cli.Create(ctx, fsmeta.CreateRequest{
				Mount:  cfg.Mount,
				Parent: root,
				Name:   name,
				Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
			})
			return err
		})
	}
	for i := 0; i < cfg.Snapshots; i++ {
		var token fsmeta.SnapshotSubtreeToken
		var snapshotErr error
		rec.recordCall("snapshot_subtree", func() error {
			token, snapshotErr = snapshotCli.SnapshotSubtree(ctx, fsmeta.SnapshotSubtreeRequest{Mount: cfg.Mount, RootInode: root})
			return snapshotErr
		})
		if snapshotErr != nil {
			continue
		}
		if token.ReadVersion != 0 {
			rec.recordCall("snapshot_readdirplus", func() error {
				_, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
					Mount:           cfg.Mount,
					Parent:          root,
					Limit:           cfg.PageLimit,
					SnapshotVersion: token.ReadVersion,
				})
				return err
			})
		}
		rec.recordCall("retire_snapshot_subtree", func() error {
			return snapshotCli.RetireSnapshotSubtree(ctx, token)
		})
	}

	return finishResult(DurableSnapshot, cfg.RunID, started, rec.snapshot())
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

// timeCall measures one logical metadata operation. Retryable transaction
// contention is part of the user-visible latency for a real client, so the
// benchmark retries it inside the same sample instead of counting scheduler
// jitter as a permanent workload failure.
func timeCall(fn func() error) (time.Duration, error) {
	start := time.Now()
	var err error
	for attempt := 0; attempt < maxOperationAttempts; attempt++ {
		err = fn()
		if !shouldRetryOperation(err) || attempt+1 == maxOperationAttempts {
			return time.Since(start), err
		}
		time.Sleep(operationRetryBackoff(attempt))
	}
	return time.Since(start), err
}

func shouldRetryOperation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return false
	}
	return nokverrors.Retryable(err)
}

func operationRetryBackoff(attempt int) time.Duration {
	backoff := operationRetryBase
	for i := 0; i < attempt; i++ {
		backoff *= 2
		if backoff >= operationRetryMax {
			return operationRetryMax
		}
	}
	return backoff
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

func (s *watchStarts) put(name string, started time.Time) {
	s.mu.Lock()
	s.values[name] = started
	s.mu.Unlock()
}

func (s *watchStarts) pop(name string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	started, ok := s.values[name]
	if ok {
		delete(s.values, name)
	}
	return started, ok
}

func (s *watchStarts) delete(name string) {
	s.mu.Lock()
	delete(s.values, name)
	s.mu.Unlock()
}

func closeWatchAfterDelivery(stream fsmetaclient.WatchSubscription, expected, delivered *atomic.Int64) {
	deadline := time.NewTimer(watchTailTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(watchDeliveryPoll)
	defer ticker.Stop()
	for {
		if delivered.Load() >= expected.Load() {
			_ = stream.Close()
			return
		}
		select {
		case <-deadline.C:
			_ = stream.Close()
			return
		case <-ticker.C:
		}
	}
}

func collectWatchEvents(ctx context.Context, stream fsmetaclient.WatchSubscription, starts *watchStarts, expected, delivered *atomic.Int64, createsDone <-chan struct{}, rec *recorder, done chan<- error) {
	received := 0
	for {
		select {
		case <-createsDone:
			if received >= int(expected.Load()) {
				done <- nil
				return
			}
			createsDone = nil
		default:
		}
		evt, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) && received >= int(expected.Load()) {
				done <- nil
				return
			}
			if errors.Is(err, io.EOF) {
				done <- fmt.Errorf("watch-subtree received %d/%d notifications before stream closed", received, expected.Load())
				return
			}
			done <- err
			return
		}
		_ = stream.Ack(evt.Cursor)
		name, ok := fsmeta.DentryNameOfKey(evt.Key)
		if !ok {
			continue
		}
		if started, ok := starts.pop(name); ok {
			rec.record("watch_notify", time.Since(started), nil)
			received++
			delivered.Add(1)
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

func waitForWatchName(ctx context.Context, stream fsmetaclient.WatchSubscription, want string) error {
	for {
		evt, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = stream.Ack(evt.Cursor)
		if name, ok := fsmeta.DentryNameOfKey(evt.Key); ok && name == want {
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
	return cfg
}

func normalizeNegativeLookupConfig(cfg NegativeLookupConfig) NegativeLookupConfig {
	if cfg.Mount == "" {
		cfg.Mount = "fsmeta-workload"
	}
	if cfg.RunID == "" {
		cfg.RunID = NewRunID()
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 4
	}
	if cfg.Keys <= 0 {
		cfg.Keys = 1024
	}
	if cfg.ReadsPerClient <= 0 {
		cfg.ReadsPerClient = 64
	}
	if cfg.Parent == 0 {
		cfg.Parent = fsmeta.RootInode
	}
	return cfg
}

func normalizeDurableSnapshotConfig(cfg DurableSnapshotConfig) DurableSnapshotConfig {
	if cfg.Mount == "" {
		cfg.Mount = "fsmeta-workload"
	}
	if cfg.RunID == "" {
		cfg.RunID = NewRunID()
	}
	if cfg.Files <= 0 {
		cfg.Files = 128
	}
	if cfg.Snapshots <= 0 {
		cfg.Snapshots = 16
	}
	if cfg.PageLimit == 0 {
		cfg.PageLimit = uint32(cfg.Files)
	}
	if cfg.PageLimit > fsmeta.MaxReadDirLimit {
		cfg.PageLimit = fsmeta.MaxReadDirLimit
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
