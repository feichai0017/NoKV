package fsmetabench

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/feichai0017/NoKV/fsmeta/workload"
)

const benchEnvKey = "NOKV_FSMETA_BENCH"

var (
	fsmetaAddr           = flag.String("fsmeta_addr", "127.0.0.1:8090", "FSMetadata gRPC endpoint")
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

	cli, err := fsmetaclient.NewGRPCClient(ctx, *fsmetaAddr)
	if err != nil {
		t.Fatalf("dial fsmeta: %v", err)
	}
	defer func() { _ = cli.Close() }()

	runID := workload.NewRunID()
	var results []workload.Result
	for _, name := range parseWorkloads(*fsmetaWorkloads) {
		var result workload.Result
		switch name {
		case workload.CheckpointStorm:
			result, err = workload.RunCheckpointStorm(ctx, cli, workload.CheckpointStormConfig{
				Mount:             fsmeta.MountID(*fsmetaMount),
				RunID:             runID,
				Clients:           *fsmetaClients,
				Directories:       *fsmetaDirs,
				FilesPerDirectory: *fsmetaFilesPerDir,
				StartInode:        fsmeta.InodeID(*fsmetaStartInode),
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
				StartInode:     fsmeta.InodeID(*fsmetaStartInode + 1_000_000),
			})
		default:
			t.Fatalf("unknown workload %q", name)
		}
		if err != nil {
			t.Fatalf("run %s: %v", name, err)
		}
		results = append(results, result)
	}

	rows := make([]workload.SummaryRow, 0)
	for _, result := range results {
		rows = append(rows, workload.SummaryRows(result)...)
	}
	output := *fsmetaOutput
	if output == "" {
		output = filepath.Join("data", "fsmeta", "results", "fsmeta_results_"+runID+".csv")
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
