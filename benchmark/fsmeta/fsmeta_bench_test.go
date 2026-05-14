// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmetabench

import (
	"context"
	"errors"
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
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const benchEnvKey = "NOKV_FSMETA_BENCH"

var (
	fsmetaAddr            = flag.String("fsmeta_addr", "127.0.0.1:8090", "FSMetadata gRPC endpoint")
	fsmetaCoordAddr       = flag.String("fsmeta_coordinator_addr", "127.0.0.1:2379", "Coordinator gRPC endpoint for mount bootstrap")
	fsmetaWorkloads       = flag.String("fsmeta_workloads", "multi-workspace-autoscale,mixed,durable-snapshot,checkpoint-storm,hotspot-fanin,watch-subtree,negative-lookup", "comma-separated workloads: multi-workspace-autoscale,mixed,durable-snapshot,checkpoint-storm,hotspot-fanin,watch-subtree,negative-lookup")
	fsmetaMount           = flag.String("fsmeta_mount", "fsmeta-bench", "fsmeta mount id")
	fsmetaClients         = flag.Int("fsmeta_clients", 8, "concurrent clients")
	fsmetaDirs            = flag.Int("fsmeta_dirs", 16, "checkpoint-storm directory count")
	fsmetaFilesPerDir     = flag.Int("fsmeta_files_per_dir", 128, "checkpoint-storm files per directory")
	fsmetaFiles           = flag.Int("fsmeta_files", 2048, "hotspot-fanin/watch/negative file count")
	fsmetaReadsPerClient  = flag.Int("fsmeta_reads_per_client", 128, "hotspot-fanin/negative reads per client")
	fsmetaPageLimit       = flag.Uint("fsmeta_page_limit", 0, "readdir page limit; 0 uses workload default")
	fsmetaReadDirPlus     = flag.Bool("fsmeta_readdirplus", true, "hotspot-fanin uses ReadDirPlus instead of ReadDir")
	fsmetaWatchWindow     = flag.Uint("fsmeta_watch_window", 0, "watch-subtree back-pressure window; 0 uses workload default")
	fsmetaMountWait       = flag.Duration("fsmeta_mount_wait", 30*time.Second, "maximum time to wait for fsmeta gateway to observe benchmark mount")
	fsmetaGroups          = flag.Int("fsmeta_groups", 4, "mixed workload group directory count")
	fsmetaEntriesPerGroup = flag.Int("fsmeta_entries_per_group", 8, "mixed workload published entry count per group")
	fsmetaArtifactsPerRun = flag.Int("fsmeta_artifacts_per_entry", 4, "mixed workload artifact file count per entry; minimum 4")
	fsmetaWorkspaces      = flag.Int("fsmeta_workspaces", 4, "multi-workspace-autoscale workspace count")
	fsmetaSessionTTL      = flag.Duration("fsmeta_session_ttl", 5*time.Minute, "mixed writer session TTL")
	fsmetaStaleSessionTTL = flag.Duration("fsmeta_stale_session_ttl", 20*time.Millisecond, "mixed stale-session cleanup TTL; keep short for throughput runs, override for long-TTL cleanup drills")
	fsmetaLookupCache     = flag.Int("fsmeta_lookup_cache_entries", 4096, "client-side positive Lookup cache entries; 0 disables")
	fsmetaLookupCacheTTL  = flag.Duration("fsmeta_lookup_cache_ttl", time.Second, "client-side positive Lookup cache TTL")
	fsmetaTimeout         = flag.Duration("fsmeta_timeout", 5*time.Minute, "overall benchmark timeout")
	fsmetaOutput          = flag.String("fsmeta_output", "", "summary CSV output path")
)

func TestBenchmarkFSMeta(t *testing.T) {
	if os.Getenv(benchEnvKey) != "1" {
		t.Skipf("set %s=1 to run fsmeta benchmarks", benchEnvKey)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *fsmetaTimeout)
	defer cancel()
	ensureBenchmarkMount(t, ctx)

	runID := workload.NewRunID()
	var results []workload.Result
	cli, cleanup := openBenchmarkClient(t, ctx)
	defer cleanup()
	waitForFSMetaMount(t, ctx, cli)
	for _, workloadName := range parseWorkloads(*fsmetaWorkloads) {
		result, err := runBenchmarkWorkload(ctx, cli, workloadName, runID)
		if err != nil {
			t.Fatalf("run %s: %v", workloadName, err)
		}
		results = append(results, result)
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

func ensureBenchmarkMount(t *testing.T, ctx context.Context) {
	t.Helper()
	if strings.TrimSpace(*fsmetaCoordAddr) == "" || strings.TrimSpace(*fsmetaMount) == "" {
		return
	}
	coordRPC, err := coordclient.NewGRPCClient(ctx, *fsmetaCoordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator for mount bootstrap: %v", err)
	}
	defer func() { _ = coordRPC.Close() }()
	resp, err := coordRPC.GetMount(ctx, &coordpb.GetMountRequest{MountId: strings.TrimSpace(*fsmetaMount)})
	if err != nil {
		t.Fatalf("get benchmark mount: %v", err)
	}
	if resp != nil && !resp.GetNotFound() {
		if resp.GetMount().GetState() == coordpb.MountState_MOUNT_STATE_RETIRED {
			t.Fatalf("benchmark mount %q is retired", *fsmetaMount)
		}
		return
	}
	alloc, err := coordRPC.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	if err != nil {
		t.Fatalf("allocate benchmark mount key id: %v", err)
	}
	if alloc == nil || alloc.GetFirstId() == 0 || alloc.GetCount() != 1 {
		t.Fatalf("coordinator returned invalid benchmark mount key id allocation")
	}
	mountKeyID := alloc.GetFirstId()
	publishResp, err := coordRPC.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.MountRegistered(strings.TrimSpace(*fsmetaMount), mountKeyID, uint64(fsmeta.RootInode), 1)),
	})
	if err != nil {
		t.Fatalf("register benchmark mount: %v", err)
	}
	if publishResp == nil || !publishResp.GetAccepted() {
		t.Fatalf("benchmark mount root event was not accepted")
	}
	// MountRegistered materializes the root subtree authority in rooted truth;
	// publishing a second explicit declaration would not be idempotent after
	// the frontier advances during a long benchmark run.
}

func openBenchmarkClient(t *testing.T, ctx context.Context) (workload.Client, func()) {
	t.Helper()
	cfg := fsmetaclient.ClientConfig{
		DisableLookupCache: *fsmetaLookupCache <= 0,
	}
	if *fsmetaLookupCache > 0 {
		cfg.LookupCache = fsmetaclient.LookupCacheConfig{
			MaxEntries: *fsmetaLookupCache,
			TTL:        *fsmetaLookupCacheTTL,
		}
	}
	cli, err := fsmetaclient.NewGRPCClientWithConfig(ctx, *fsmetaAddr, cfg)
	if err != nil {
		t.Fatalf("dial fsmeta: %v", err)
	}
	return cli, func() { _ = cli.Close() }
}

func waitForFSMetaMount(t *testing.T, ctx context.Context, cli workload.Client) {
	t.Helper()
	watchCli, ok := cli.(workload.WatchClient)
	if !ok || *fsmetaMountWait <= 0 {
		return
	}
	deadline := time.Now().Add(*fsmetaMountWait)
	var lastErr error
	for {
		stream, err := watchCli.WatchSubtree(ctx, fsmeta.WatchRequest{
			Mount:     fsmeta.MountID(*fsmetaMount),
			RootInode: fsmeta.RootInode,
		})
		if err == nil {
			_ = stream.Close()
			return
		}
		lastErr = err
		if !isMountVisibilityPending(err) {
			t.Fatalf("wait for fsmeta mount visibility: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for fsmeta gateway to observe mount %q: %v", *fsmetaMount, lastErr)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for fsmeta mount visibility: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func isMountVisibilityPending(err error) bool {
	return errors.Is(err, fsmeta.ErrMountNotRegistered)
}

func runBenchmarkWorkload(ctx context.Context, cli workload.Client, workloadName, runID string) (workload.Result, error) {
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
		})
	case workload.WatchSubtree:
		result, err = workload.RunWatchSubtree(ctx, cli, workload.WatchSubtreeConfig{
			Mount:              fsmeta.MountID(*fsmetaMount),
			RunID:              runID,
			Clients:            *fsmetaClients,
			Files:              *fsmetaFiles,
			BackPressureWindow: uint32(*fsmetaWatchWindow),
		})
	case workload.DurableSnapshot:
		result, err = workload.RunDurableSnapshot(ctx, cli, workload.DurableSnapshotConfig{
			Mount:     fsmeta.MountID(*fsmetaMount),
			RunID:     runID,
			Clients:   *fsmetaClients,
			Files:     *fsmetaFilesPerDir,
			Snapshots: *fsmetaEntriesPerGroup,
			PageLimit: uint32(*fsmetaPageLimit),
		})
	case workload.NegativeLookup:
		result, err = workload.RunNegativeLookup(ctx, cli, workload.NegativeLookupConfig{
			Mount:          fsmeta.MountID(*fsmetaMount),
			RunID:          runID,
			Clients:        *fsmetaClients,
			Keys:           *fsmetaFiles,
			ReadsPerClient: *fsmetaReadsPerClient,
			Parent:         fsmeta.RootInode,
		})
	case workload.Mixed:
		result, err = workload.RunMixed(ctx, cli, workload.MixedConfig{
			Mount:           fsmeta.MountID(*fsmetaMount),
			RunID:           runID,
			Clients:         *fsmetaClients,
			Groups:          *fsmetaGroups,
			EntriesPerGroup: *fsmetaEntriesPerGroup,
			ArtifactsPerRun: *fsmetaArtifactsPerRun,
			PageLimit:       uint32(*fsmetaPageLimit),
			SessionTTL:      *fsmetaSessionTTL,
			StaleSessionTTL: *fsmetaStaleSessionTTL,
		})
	case workload.MultiWorkspaceAutoscale:
		result, err = workload.RunMultiWorkspaceAutoscale(ctx, cli, workload.MultiWorkspaceAutoscaleConfig{
			MixedConfig: workload.MixedConfig{
				Mount:           fsmeta.MountID(*fsmetaMount),
				RunID:           runID,
				Clients:         *fsmetaClients,
				Groups:          *fsmetaGroups,
				EntriesPerGroup: *fsmetaEntriesPerGroup,
				ArtifactsPerRun: *fsmetaArtifactsPerRun,
				PageLimit:       uint32(*fsmetaPageLimit),
				SessionTTL:      *fsmetaSessionTTL,
				StaleSessionTTL: *fsmetaStaleSessionTTL,
			},
			Workspaces: *fsmetaWorkspaces,
		})
	default:
		return workload.Result{}, fmt.Errorf("unknown workload %q", workloadName)
	}
	result.Driver = workload.DriverNativeFSMetadata
	return result, err
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
