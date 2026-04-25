package fsmetabench

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/benchmark/fsmeta/workload"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	raftclient "github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const benchEnvKey = "NOKV_FSMETA_BENCH"

var (
	fsmetaDrivers        = flag.String("fsmeta_drivers", workload.DriverNativeFSMetadata, "comma-separated drivers: native-fsmeta,generic-kv")
	fsmetaAddr           = flag.String("fsmeta_addr", "127.0.0.1:8090", "FSMetadata gRPC endpoint")
	fsmetaCoordAddr      = flag.String("fsmeta_coordinator_addr", "127.0.0.1:2379", "Coordinator gRPC endpoint for generic-kv driver")
	fsmetaWorkloads      = flag.String("fsmeta_workloads", "checkpoint-storm,hotspot-fanin", "comma-separated workloads")
	fsmetaMount          = flag.String("fsmeta_mount", "fsmeta-bench", "fsmeta mount id")
	fsmetaClients        = flag.Int("fsmeta_clients", 8, "concurrent clients")
	fsmetaDirs           = flag.Int("fsmeta_dirs", 16, "checkpoint-storm directory count")
	fsmetaFilesPerDir    = flag.Int("fsmeta_files_per_dir", 128, "checkpoint-storm files per directory")
	fsmetaFiles          = flag.Int("fsmeta_files", 2048, "hotspot-fanin file count")
	fsmetaReadsPerClient = flag.Int("fsmeta_reads_per_client", 128, "hotspot-fanin reads per client")
	fsmetaPageLimit      = flag.Uint("fsmeta_page_limit", 0, "readdir page limit; 0 uses workload default")
	fsmetaReadDirPlus    = flag.Bool("fsmeta_readdirplus", true, "hotspot-fanin uses ReadDirPlus instead of ReadDir")
	fsmetaStartInode     = flag.Uint64("fsmeta_start_inode", 10_000_000, "first inode id used by generated metadata")
	fsmetaTimeout        = flag.Duration("fsmeta_timeout", 5*time.Minute, "overall benchmark timeout")
	fsmetaOutput         = flag.String("fsmeta_output", "", "summary CSV output path")
)

func TestBenchmarkFSMeta(t *testing.T) {
	if os.Getenv(benchEnvKey) != "1" {
		t.Skipf("set %s=1 to run fsmeta benchmarks", benchEnvKey)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *fsmetaTimeout)
	defer cancel()

	runID := workload.NewRunID()
	var results []workload.Result
	for driverIndex, driverName := range parseDrivers(*fsmetaDrivers) {
		cli, cleanup := openBenchmarkClient(t, ctx, driverName)
		defer cleanup()
		driverRunID := runID + "-" + driverName
		startInode := fsmeta.InodeID(*fsmetaStartInode + uint64(driverIndex)*10_000_000)
		for _, workloadName := range parseWorkloads(*fsmetaWorkloads) {
			result, err := runBenchmarkWorkload(ctx, cli, driverName, workloadName, driverRunID, startInode)
			if err != nil {
				t.Fatalf("run %s/%s: %v", driverName, workloadName, err)
			}
			results = append(results, result)
		}
	}

	rows := make([]workload.SummaryRow, 0)
	for _, result := range results {
		rows = append(rows, workload.SummaryRows(result)...)
	}
	output := *fsmetaOutput
	if output == "" {
		output = filepath.Join("..", "data", "fsmeta", "results", "fsmeta_results_"+runID+".csv")
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		t.Fatalf("mkdir output dir: %v", err)
	}
	f, err := os.Create(output)
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := workload.WriteSummaryCSV(f, rows); err != nil {
		t.Fatalf("write summary CSV: %v", err)
	}
	t.Logf("wrote fsmeta benchmark summary: %s", output)
}

func openBenchmarkClient(t *testing.T, ctx context.Context, driverName string) (workload.Client, func()) {
	t.Helper()
	switch driverName {
	case workload.DriverNativeFSMetadata:
		cli, err := fsmetaclient.NewGRPCClient(ctx, *fsmetaAddr)
		if err != nil {
			t.Fatalf("dial fsmeta: %v", err)
		}
		return cli, func() { _ = cli.Close() }
	case workload.DriverGenericKV:
		if strings.TrimSpace(*fsmetaCoordAddr) == "" {
			t.Fatalf("fsmeta_coordinator_addr is required for %s", workload.DriverGenericKV)
		}
		coordRPC, err := coordclient.NewGRPCClient(ctx, *fsmetaCoordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial coordinator: %v", err)
		}
		kv, err := raftclient.New(raftclient.Config{
			Context:        ctx,
			StoreResolver:  coordRPC,
			RegionResolver: coordRPC,
			DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		})
		if err != nil {
			_ = coordRPC.Close()
			t.Fatalf("open raftstore client: %v", err)
		}
		runner, err := fsmetaexec.NewRaftstoreRunner(kv, coordRPC)
		if err != nil {
			_ = kv.Close()
			t.Fatalf("open raftstore runner: %v", err)
		}
		cli, err := workload.NewGenericKVDriver(runner)
		if err != nil {
			_ = kv.Close()
			t.Fatalf("open generic-kv driver: %v", err)
		}
		return cli, func() { _ = kv.Close() }
	default:
		t.Fatalf("unknown fsmeta driver %q", driverName)
		return nil, nil
	}
}

func runBenchmarkWorkload(ctx context.Context, cli workload.Client, driverName, workloadName, runID string, startInode fsmeta.InodeID) (workload.Result, error) {
	var (
		result workload.Result
		err    error
	)
	switch workloadName {
	case workload.CheckpointStorm:
		result, err = workload.RunCheckpointStorm(ctx, cli, workload.CheckpointStormConfig{
			Mount:             fsmeta.MountID(*fsmetaMount),
			RunID:             runID,
			Clients:           *fsmetaClients,
			Directories:       *fsmetaDirs,
			FilesPerDirectory: *fsmetaFilesPerDir,
			StartInode:        startInode,
		})
	case workload.HotspotFanIn:
		result, err = workload.RunHotspotFanIn(ctx, cli, workload.HotspotFanInConfig{
			Mount:          fsmeta.MountID(*fsmetaMount),
			RunID:          runID,
			Clients:        *fsmetaClients,
			Files:          *fsmetaFiles,
			ReadsPerClient: *fsmetaReadsPerClient,
			PageLimit:      uint32(*fsmetaPageLimit),
			ReadDirPlus:    *fsmetaReadDirPlus,
			StartInode:     startInode + 1_000_000,
		})
	default:
		return workload.Result{}, fmt.Errorf("unknown workload %q", workloadName)
	}
	result.Driver = driverName
	return result, err
}

func parseDrivers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name != "" {
			out = append(out, name)
		}
	}
	if len(out) == 0 {
		return []string{workload.DriverNativeFSMetadata}
	}
	return out
}

func parseWorkloads(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name != "" {
			out = append(out, name)
		}
	}
	if len(out) == 0 {
		return []string{workload.CheckpointStorm}
	}
	return out
}
