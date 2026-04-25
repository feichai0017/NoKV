package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/feichai0017/NoKV/benchmark/fsmeta/workload"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
)

func main() {
	var (
		addr           = flag.String("addr", "127.0.0.1:8090", "FSMetadata gRPC endpoint")
		workloadName   = flag.String("workload", workload.CheckpointStorm, "workload: checkpoint-storm|hotspot-fanin|watch-subtree")
		mount          = flag.String("mount", "fsmeta-demo", "fsmeta mount id")
		runID          = flag.String("run-id", "", "run id suffix; defaults to current UTC timestamp")
		clients        = flag.Int("clients", 4, "concurrent clients")
		dirs           = flag.Int("dirs", 8, "checkpoint-storm directory count")
		filesPerDir    = flag.Int("files-per-dir", 128, "checkpoint-storm files per directory")
		files          = flag.Int("files", 1024, "hotspot-fanin file count")
		readsPerClient = flag.Int("reads-per-client", 64, "hotspot-fanin reads per client")
		pageLimit      = flag.Uint("page-limit", 0, "readdir page limit; 0 uses workload default")
		watchWindow    = flag.Uint("watch-window", 0, "watch-subtree back-pressure window; 0 uses workload default")
		useReadDirPlus = flag.Bool("readdirplus", true, "hotspot-fanin uses ReadDirPlus instead of ReadDir")
		startInode     = flag.Uint64("start-inode", 1_000_000, "first inode id used by generated metadata")
		timeout        = flag.Duration("timeout", 2*time.Minute, "overall workload timeout")
		printCSV       = flag.Bool("csv", false, "print summary as CSV instead of a table")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cli, err := fsmetaclient.NewGRPCClient(ctx, *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial fsmeta: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = cli.Close() }()

	id := *runID
	if id == "" {
		id = workload.NewRunID()
	}
	result, err := run(ctx, cli, runConfig{
		name:           *workloadName,
		mount:          fsmeta.MountID(*mount),
		runID:          id,
		clients:        *clients,
		dirs:           *dirs,
		filesPerDir:    *filesPerDir,
		files:          *files,
		readsPerClient: *readsPerClient,
		pageLimit:      uint32(*pageLimit),
		watchWindow:    uint32(*watchWindow),
		readDirPlus:    *useReadDirPlus,
		startInode:     fsmeta.InodeID(*startInode),
	})
	if *printCSV {
		_ = workload.WriteSummaryCSV(os.Stdout, workload.SummaryRows(result))
	} else {
		printSummary(result)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "workload failed: %v\n", err)
		os.Exit(1)
	}
}

type runConfig struct {
	name           string
	mount          fsmeta.MountID
	runID          string
	clients        int
	dirs           int
	filesPerDir    int
	files          int
	readsPerClient int
	pageLimit      uint32
	watchWindow    uint32
	readDirPlus    bool
	startInode     fsmeta.InodeID
}

func run(ctx context.Context, cli workload.Client, cfg runConfig) (workload.Result, error) {
	switch cfg.name {
	case workload.CheckpointStorm:
		return workload.RunCheckpointStorm(ctx, cli, workload.CheckpointStormConfig{
			Mount:             cfg.mount,
			RunID:             cfg.runID,
			Clients:           cfg.clients,
			Directories:       cfg.dirs,
			FilesPerDirectory: cfg.filesPerDir,
			StartInode:        cfg.startInode,
		})
	case workload.HotspotFanIn:
		return workload.RunHotspotFanIn(ctx, cli, workload.HotspotFanInConfig{
			Mount:          cfg.mount,
			RunID:          cfg.runID,
			Clients:        cfg.clients,
			Files:          cfg.files,
			ReadsPerClient: cfg.readsPerClient,
			PageLimit:      cfg.pageLimit,
			ReadDirPlus:    cfg.readDirPlus,
			StartInode:     cfg.startInode,
		})
	case workload.WatchSubtree:
		return workload.RunWatchSubtree(ctx, cli, workload.WatchSubtreeConfig{
			Mount:              cfg.mount,
			RunID:              cfg.runID,
			Clients:            cfg.clients,
			Files:              cfg.files,
			StartInode:         cfg.startInode,
			BackPressureWindow: cfg.watchWindow,
		})
	default:
		return workload.Result{}, fmt.Errorf("unknown workload %q", cfg.name)
	}
}

func printSummary(result workload.Result) {
	fmt.Printf("workload=%s run_id=%s ops=%d errors=%d duration=%s\n", result.Name, result.RunID, result.Ops, result.Errors, result.Duration)
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "operation\tcount\terrors\tops/s\tavg_us\tp50_us\tp95_us\tp99_us")
	for _, row := range workload.SummaryRows(result) {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.2f\t%.1f\t%.1f\t%.1f\t%.1f\n",
			row.Operation,
			row.Count,
			row.Errors,
			row.Throughput,
			row.AverageUS,
			row.P50US,
			row.P95US,
			row.P99US,
		)
	}
	_ = tw.Flush()
}
