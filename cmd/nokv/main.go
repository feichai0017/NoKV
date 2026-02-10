package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/feichai0017/hotring"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
)

var exit = os.Exit
var stat = os.Stat

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stdout)
		exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "stats":
		err = runStatsCmd(os.Stdout, args)
	case "manifest":
		err = runManifestCmd(os.Stdout, args)
	case "vlog":
		err = runVlogCmd(os.Stdout, args)
	case "regions":
		err = runRegionsCmd(os.Stdout, args)
	case "scheduler":
		err = runSchedulerCmd(os.Stdout, args)
	case "serve":
		err = runServeCmd(os.Stdout, args)
	case "help", "-h", "--help":
		printUsage(os.Stdout)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exit(1)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage: nokv <command> [flags]

Commands:
  stats     Dump runtime backlog metrics (requires working directory or expvar endpoint)
  manifest  Inspect manifest state, levels, and value log metadata
  vlog      List value log segments and active head
  regions   Show region metadata catalog from manifest/store
  scheduler Display scheduler heartbeat snapshot (in-process only)
  serve     Start TinyKv gRPC service backed by a local raftstore

Run "nokv <command> -h" for command-specific flags.`)
}

func runStatsCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory (offline snapshot)")
	expvarURL := fs.String("expvar", "", "HTTP endpoint exposing /debug/vars (overrides workdir)")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	noMetrics := fs.Bool("no-region-metrics", false, "do not attach region metrics recorder (requires --workdir)")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	var snap NoKV.StatsSnapshot
	var err error
	switch {
	case *expvarURL != "":
		snap, err = fetchExpvarSnapshot(*expvarURL)
	case *workDir != "":
		snap, err = localStatsSnapshot(*workDir, !*noMetrics)
	default:
		return fmt.Errorf("either --workdir or --expvar must be specified")
	}
	if err != nil {
		return err
	}
	return renderStats(w, snap, *asJSON)
}

func renderStats(w io.Writer, snap NoKV.StatsSnapshot, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(snap)
	}

	fmt.Fprintf(w, "Entries               %d\n", snap.Entries)
	fmt.Fprintf(w, "Flush.Pending          %d\n", snap.Flush.Pending)
	fmt.Fprintf(w, "Compaction.Backlog     %d\n", snap.Compaction.Backlog)
	fmt.Fprintf(w, "Compaction.MaxScore    %.2f\n", snap.Compaction.MaxScore)
	fmt.Fprintf(w, "Flush.Wait.LastMs      %.2f\n", snap.Flush.LastWaitMs)
	fmt.Fprintf(w, "Flush.Wait.MaxMs       %.2f\n", snap.Flush.MaxWaitMs)
	fmt.Fprintf(w, "Flush.Build.LastMs     %.2f\n", snap.Flush.LastBuildMs)
	fmt.Fprintf(w, "Flush.Build.MaxMs      %.2f\n", snap.Flush.MaxBuildMs)
	fmt.Fprintf(w, "Flush.Release.LastMs   %.2f\n", snap.Flush.LastReleaseMs)
	fmt.Fprintf(w, "Flush.Release.MaxMs    %.2f\n", snap.Flush.MaxReleaseMs)
	fmt.Fprintf(w, "Compaction.LastMs      %.2f\n", snap.Compaction.LastDurationMs)
	fmt.Fprintf(w, "Compaction.MaxMs       %.2f\n", snap.Compaction.MaxDurationMs)
	fmt.Fprintf(w, "Compaction.Runs        %d\n", snap.Compaction.Runs)
	fmt.Fprintf(w, "ValueLog.Segments      %d\n", snap.ValueLog.Segments)
	fmt.Fprintf(w, "ValueLog.PendingDelete %d\n", snap.ValueLog.PendingDeletes)
	fmt.Fprintf(w, "ValueLog.DiscardQueue  %d\n", snap.ValueLog.DiscardQueue)
	if snap.ValueLog.GC.GCRuns > 0 || snap.ValueLog.GC.GCScheduled > 0 {
		fmt.Fprintf(w, "ValueLog.GC            runs=%d scheduled=%d active=%d removed=%d skipped=%d throttled=%d rejected=%d parallel=%d\n",
			snap.ValueLog.GC.GCRuns,
			snap.ValueLog.GC.GCScheduled,
			snap.ValueLog.GC.GCActive,
			snap.ValueLog.GC.SegmentsRemoved,
			snap.ValueLog.GC.GCSkipped,
			snap.ValueLog.GC.GCThrottled,
			snap.ValueLog.GC.GCRejected,
			snap.ValueLog.GC.GCParallelism,
		)
	}
	if len(snap.ValueLog.Heads) > 0 {
		buckets := make([]uint32, 0, len(snap.ValueLog.Heads))
		for bucket := range snap.ValueLog.Heads {
			buckets = append(buckets, bucket)
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
		for _, bucket := range buckets {
			head := snap.ValueLog.Heads[bucket]
			if head.IsZero() {
				continue
			}
			fmt.Fprintf(w, "ValueLog.Head[%d]       fid=%d offset=%d len=%d\n",
				bucket, head.Fid, head.Offset, head.Len)
		}
	}
	fmt.Fprintf(w, "Write.HotKeyThrottled  %d\n", snap.Write.HotKeyLimited)
	if snap.Hot.ReadRing != nil {
		hs := snap.Hot.ReadRing
		fmt.Fprintf(w, "HotRing.Buckets        %d\n", hs.Buckets)
		fmt.Fprintf(w, "HotRing.Nodes          %d (load=%.2f)\n", hs.Nodes, hs.LoadFactor)
		fmt.Fprintf(w, "HotRing.Touches        %d (clamps=%d inserts=%d removes=%d)\n",
			hs.Touches, hs.Clamps, hs.Inserts, hs.Removes)
		if hs.WindowSlots > 0 && hs.WindowSlotDuration > 0 {
			fmt.Fprintf(w, "HotRing.Window         slots=%d dur=%s\n",
				hs.WindowSlots, hs.WindowSlotDuration.String())
		}
		if hs.DecayInterval > 0 && hs.DecayShift > 0 {
			fmt.Fprintf(w, "HotRing.Decay          every=%s shift=%d\n",
				hs.DecayInterval.String(), hs.DecayShift)
		}
	}
	fmt.Fprintf(w, "Compaction.ValueWeight %.2f", snap.Compaction.ValueWeight)
	if snap.Compaction.ValueWeightSuggested > snap.Compaction.ValueWeight {
		fmt.Fprintf(w, " (suggested %.2f)", snap.Compaction.ValueWeightSuggested)
	}
	fmt.Fprintln(w)
	if snap.LSM.ValueDensityMax > 0 {
		fmt.Fprintf(w, "LSM.ValueDensityMax    %.2f\n", snap.LSM.ValueDensityMax)
	}
	if snap.LSM.ValueDensityAlert {
		fmt.Fprintln(w, "LSM.ValueDensityAlert  true")
	}
	fmt.Fprintf(w, "WAL.ActiveSegment      %d (segments=%d removed=%d)\n", snap.WAL.ActiveSegment, snap.WAL.SegmentCount, snap.WAL.SegmentsRemoved)
	fmt.Fprintf(w, "WAL.ActiveSize         %d bytes\n", snap.WAL.ActiveSize)
	if snap.WAL.RecordCounts.Total() > 0 {
		r := snap.WAL.RecordCounts
		fmt.Fprintf(w, "WAL.Records            entries=%d raft_entries=%d raft_states=%d raft_snapshots=%d other=%d\n",
			r.Entries, r.RaftEntries, r.RaftStates, r.RaftSnapshots, r.Other)
	}
	fmt.Fprintf(w, "WAL.RaftSegments       %d (removable=%d)\n", snap.WAL.SegmentsWithRaftRecords, snap.WAL.RemovableRaftSegments)
	if snap.WAL.TypedRecordRatio > 0 || snap.WAL.TypedRecordWarning {
		fmt.Fprintf(w, "WAL.TypedRatio         %.2f\n", snap.WAL.TypedRecordRatio)
	}
	if snap.WAL.TypedRecordWarning && snap.WAL.TypedRecordReason != "" {
		fmt.Fprintf(w, "WAL.Warning            %s\n", snap.WAL.TypedRecordReason)
	}
	if snap.WAL.AutoGCRuns > 0 || snap.WAL.AutoGCRemoved > 0 || snap.WAL.AutoGCLastUnix > 0 {
		last := "never"
		if snap.WAL.AutoGCLastUnix > 0 {
			last = time.Unix(snap.WAL.AutoGCLastUnix, 0).Format(time.RFC3339)
		}
		fmt.Fprintf(w, "WAL.AutoGC             runs=%d removed=%d last=%s\n", snap.WAL.AutoGCRuns, snap.WAL.AutoGCRemoved, last)
	}
	if snap.Raft.GroupCount > 0 {
		fmt.Fprintf(w, "Raft.Groups            %d lagging=%d maxLagSegments=%d\n",
			snap.Raft.GroupCount, snap.Raft.LaggingGroups, snap.Raft.MaxLagSegments)
		fmt.Fprintf(w, "Raft.SegmentRange      min=%d max=%d\n", snap.Raft.MinLogSegment, snap.Raft.MaxLogSegment)
		if snap.Raft.LagWarnThreshold > 0 {
			fmt.Fprintf(w, "Raft.LagThreshold      %d segments\n", snap.Raft.LagWarnThreshold)
		}
		if snap.Raft.LagWarning {
			fmt.Fprintf(w, "Raft.Warning           lagging=%d maxLag=%d (threshold=%d)\n",
				snap.Raft.LaggingGroups, snap.Raft.MaxLagSegments, snap.Raft.LagWarnThreshold)
		}
	}
	fmt.Fprintf(w, "Txns.Active            %d\n", snap.Txn.Active)
	fmt.Fprintf(w, "Txns.StartedTotal      %d\n", snap.Txn.Started)
	fmt.Fprintf(w, "Txns.CommittedTotal    %d\n", snap.Txn.Committed)
	fmt.Fprintf(w, "Txns.ConflictsTotal    %d\n", snap.Txn.Conflicts)
	fmt.Fprintf(w, "Regions.Total          %d (new=%d running=%d removing=%d tombstone=%d other=%d)\n",
		snap.Region.Total, snap.Region.New, snap.Region.Running, snap.Region.Removing, snap.Region.Tombstone, snap.Region.Other)
	if snap.LSM.ValueBytesTotal > 0 {
		fmt.Fprintf(w, "LSM.ValueBytesTotal   %d\n", snap.LSM.ValueBytesTotal)
	}
	if len(snap.LSM.Levels) > 0 {
		fmt.Fprintln(w, "LSM.Levels:")
		for _, lvl := range snap.LSM.Levels {
			fmt.Fprintf(w, "  - L%d tables=%d size=%dB value=%dB stale=%dB",
				lvl.Level, lvl.TableCount, lvl.SizeBytes, lvl.ValueBytes, lvl.StaleBytes)
			if lvl.IngestTables > 0 {
				fmt.Fprintf(w, " ingestTables=%d ingestSize=%dB ingestValue=%dB",
					lvl.IngestTables, lvl.IngestSizeBytes, lvl.IngestValueBytes)
			}
			fmt.Fprintln(w)
		}
	}
	if len(snap.LSM.ColumnFamilies) > 0 {
		fmt.Fprintln(w, "ColumnFamilies:")
		var names []string
		for name := range snap.LSM.ColumnFamilies {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			cf := snap.LSM.ColumnFamilies[name]
			fmt.Fprintf(w, "  - %s: reads=%d writes=%d\n", name, cf.Reads, cf.Writes)
		}
	}
	if len(snap.Hot.ReadKeys) > 0 {
		fmt.Fprintln(w, "HotKeys:")
		for _, hk := range snap.Hot.ReadKeys {
			fmt.Fprintf(w, "  - key=%q count=%d\n", hk.Key, hk.Count)
		}
	}
	if snap.Transport.SendAttempts > 0 || snap.Transport.DialsTotal > 0 {
		fmt.Fprintf(w, "Transport.GRPC         sends=%d success=%d fail=%d retries=%d blocked=%d watchdog=%v\n",
			snap.Transport.SendAttempts,
			snap.Transport.SendSuccesses,
			snap.Transport.SendFailures,
			snap.Transport.Retries,
			snap.Transport.BlockedPeers,
			snap.Transport.WatchdogActive,
		)
	}
	if snap.Redis.CommandsTotal > 0 || snap.Redis.ConnectionsAccepted > 0 {
		fmt.Fprintf(w, "Redis.Gateway          commands=%d errors=%d active_conn=%d accepted_conn=%d\n",
			snap.Redis.CommandsTotal,
			snap.Redis.ErrorsTotal,
			snap.Redis.ConnectionsActive,
			snap.Redis.ConnectionsAccepted,
		)
	}
	return nil
}

func runManifestCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("manifest", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}
	if err := ensureManifestExists(*workDir); err != nil {
		return err
	}

	mgr, err := manifest.Open(*workDir)
	if err != nil {
		return err
	}
	defer mgr.Close()

	version := mgr.Current()
	out := map[string]any{
		"log_pointer": map[string]any{
			"segment": version.LogSegment,
			"offset":  version.LogOffset,
		},
	}
	heads := mgr.ValueLogHead()
	if len(heads) > 0 {
		buckets := make([]uint32, 0, len(heads))
		for bucket := range heads {
			buckets = append(buckets, bucket)
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
		valueLogHeads := make([]map[string]any, 0, len(buckets))
		for _, bucket := range buckets {
			meta := heads[bucket]
			valueLogHeads = append(valueLogHeads, map[string]any{
				"bucket": bucket,
				"fid":    meta.FileID,
				"offset": meta.Offset,
				"valid":  meta.Valid,
			})
		}
		out["value_log_heads"] = valueLogHeads
	}

	levelInfo := make([]map[string]any, 0, len(version.Levels))
	var levels []int
	for level := range version.Levels {
		levels = append(levels, level)
	}
	sort.Ints(levels)
	for _, level := range levels {
		files := version.Levels[level]
		totalVal := totalValue(files)
		levelInfo = append(levelInfo, map[string]any{
			"level":       level,
			"file_count":  len(files),
			"file_ids":    fileIDs(files),
			"total_bytes": totalSize(files),
			"value_bytes": totalVal,
		})
	}
	out["levels"] = levelInfo

	var valueLogs []map[string]any
	var ids []manifest.ValueLogID
	for id := range version.ValueLogs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].Bucket == ids[j].Bucket {
			return ids[i].FileID < ids[j].FileID
		}
		return ids[i].Bucket < ids[j].Bucket
	})
	for _, id := range ids {
		meta := version.ValueLogs[id]
		valueLogs = append(valueLogs, map[string]any{
			"bucket": id.Bucket,
			"fid":    id.FileID,
			"offset": meta.Offset,
			"valid":  meta.Valid,
		})
	}
	out["value_logs"] = valueLogs

	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	fmt.Fprintf(w, "Manifest Log Pointer : segment=%d offset=%d\n", version.LogSegment, version.LogOffset)
	if heads != nil {
		buckets := make([]uint32, 0, len(heads))
		for bucket := range heads {
			buckets = append(buckets, bucket)
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
		for _, bucket := range buckets {
			meta := heads[bucket]
			fmt.Fprintf(w, "ValueLog Head[%d]     : fid=%d offset=%d valid=%v\n", bucket, meta.FileID, meta.Offset, meta.Valid)
		}
	}
	fmt.Fprintln(w, "Levels:")
	for _, lvl := range levelInfo {
		fmt.Fprintf(w, "  - L%d files=%d total=%d bytes value=%d bytes ids=%v\n",
			lvl["level"], lvl["file_count"], lvl["total_bytes"], lvl["value_bytes"], lvl["file_ids"])
	}
	fmt.Fprintln(w, "ValueLog segments:")
	for _, vl := range valueLogs {
		fmt.Fprintf(w, "  - bucket=%d fid=%d offset=%d valid=%v\n", vl["bucket"], vl["fid"], vl["offset"], vl["valid"])
	}
	return nil
}

func runVlogCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("vlog", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}

	vlogDir := filepath.Join(*workDir, "vlog")
	if _, err := os.Stat(vlogDir); err != nil {
		return fmt.Errorf("vlog directory not found: %s", vlogDir)
	}

	bucketDirs, _ := filepath.Glob(filepath.Join(vlogDir, "bucket-*"))
	if len(bucketDirs) == 0 {
		manager, err := vlogpkg.Open(vlogpkg.Config{Dir: vlogDir})
		if err != nil {
			return err
		}
		defer manager.Close()

		head := manager.Head()
		fids := manager.ListFIDs()
		sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })

		out := map[string]any{
			"active_fid": manager.ActiveFID(),
			"head": map[string]any{
				"fid":    head.Fid,
				"offset": head.Offset,
			},
			"segments": fids,
		}

		if *asJSON {
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		}

		fmt.Fprintf(w, "Active FID : %d\n", manager.ActiveFID())
		fmt.Fprintf(w, "Head       : fid=%d offset=%d\n", head.Fid, head.Offset)
		fmt.Fprintf(w, "Segments   : %v\n", fids)
		return nil
	}

	sort.Strings(bucketDirs)
	bucketInfo := make([]map[string]any, 0, len(bucketDirs))
	for _, dir := range bucketDirs {
		base := filepath.Base(dir)
		var bucket int
		if _, err := fmt.Sscanf(base, "bucket-%03d", &bucket); err != nil {
			continue
		}
		manager, err := vlogpkg.Open(vlogpkg.Config{Dir: dir, Bucket: uint32(bucket)})
		if err != nil {
			return err
		}
		head := manager.Head()
		fids := manager.ListFIDs()
		sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })
		bucketInfo = append(bucketInfo, map[string]any{
			"bucket":     bucket,
			"active_fid": manager.ActiveFID(),
			"head": map[string]any{
				"fid":    head.Fid,
				"offset": head.Offset,
			},
			"segments": fids,
		})
		_ = manager.Close()
	}

	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{"buckets": bucketInfo})
	}
	for _, info := range bucketInfo {
		fmt.Fprintf(w, "Bucket %d\n", info["bucket"])
		fmt.Fprintf(w, "  Active FID : %d\n", info["active_fid"])
		head := info["head"].(map[string]any)
		fmt.Fprintf(w, "  Head       : fid=%d offset=%d\n", head["fid"], head["offset"])
		fmt.Fprintf(w, "  Segments   : %v\n", info["segments"])
	}
	return nil
}

func runRegionsCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("regions", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}

	mgr, err := manifest.Open(*workDir)
	if err != nil {
		return err
	}
	defer mgr.Close()

	snapshot := mgr.RegionSnapshot()
	regions := make([]manifest.RegionMeta, 0, len(snapshot))
	for _, meta := range snapshot {
		regions = append(regions, meta)
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i].ID < regions[j].ID })

	if *asJSON {
		out := map[string]any{
			"regions": regions,
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	if len(regions) == 0 {
		fmt.Fprintln(w, "Regions: (none)")
		return nil
	}

	fmt.Fprintln(w, "Regions:")
	for _, meta := range regions {
		fmt.Fprintf(w, "  - id=%d state=%s epoch={ver:%d conf:%d} range=[%q,%q) peers=%s\n",
			meta.ID, formatRegionState(meta.State), meta.Epoch.Version, meta.Epoch.ConfVersion,
			meta.StartKey, meta.EndKey, formatPeers(meta.Peers))
	}
	return nil
}

func runSchedulerCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	stores := storepkg.Stores()
	if len(stores) == 0 {
		return fmt.Errorf("no registered store; run inside a process hosting raftstore")
	}
	snap := stores[0].SchedulerSnapshot()
	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(snap)
	}
	fmt.Fprintf(w, "Stores (%d)\n", len(snap.Stores))
	for _, st := range snap.Stores {
		updated := ""
		if !st.UpdatedAt.IsZero() {
			updated = st.UpdatedAt.Format(time.RFC3339)
		}
		fmt.Fprintf(w, "  - store=%d region_num=%d leader_num=%d capacity=%d available=%d updated=%s\n",
			st.StoreID, st.RegionNum, st.LeaderNum, st.Capacity, st.Available, updated)
	}
	if len(snap.Stores) > 0 {
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "Regions (%d)\n", len(snap.Regions))
	for _, region := range snap.Regions {
		fmt.Fprintf(w, "  - region=%d", region.ID)
		if !region.LastHeartbeat.IsZero() {
			fmt.Fprintf(w, " last_heartbeat=%s lag=%s", region.LastHeartbeat.Format(time.RFC3339), region.Lag)
		}
		fmt.Fprint(w, " peers=")
		for i, peer := range region.Peers {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, "%d/%d", peer.StoreID, peer.PeerID)
		}
		fmt.Fprintln(w)
	}
	return nil
}

func localStatsSnapshot(workDir string, attachMetrics bool) (NoKV.StatsSnapshot, error) {
	if workDir == "" {
		return NoKV.StatsSnapshot{}, fmt.Errorf("workdir is required")
	}
	opts := NoKV.NewDefaultOptions()
	opts.WorkDir = workDir
	db := NoKV.Open(opts)
	defer func() {
		_ = db.Close()
	}()
	if attachMetrics {
		if metrics := firstRegionMetrics(); metrics != nil {
			db.SetRegionMetrics(metrics)
		}
	}
	return db.Info().Snapshot(), nil
}

func fetchExpvarSnapshot(url string) (NoKV.StatsSnapshot, error) {
	if !strings.Contains(url, "://") {
		url = "http://" + url
	}
	if !strings.Contains(url, "/debug/vars") {
		if strings.HasSuffix(url, "/") {
			url += "debug/vars"
		} else {
			url += "/debug/vars"
		}
	}
	resp, err := http.Get(url) // #nosec G107 - CLI utility, user-provided URL.
	if err != nil {
		return NoKV.StatsSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return NoKV.StatsSnapshot{}, fmt.Errorf("expvar request failed: %s", resp.Status)
	}
	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return NoKV.StatsSnapshot{}, err
	}
	return parseExpvarSnapshot(data), nil
}

func parseExpvarSnapshot(data map[string]any) NoKV.StatsSnapshot {
	var snap NoKV.StatsSnapshot
	setInt := func(key string, dest *int64) {
		if v, ok := data[key]; ok {
			switch val := v.(type) {
			case float64:
				*dest = int64(val)
			case map[string]any:
				if inner, ok := val["value"]; ok {
					switch n := inner.(type) {
					case float64:
						*dest = int64(n)
					}
				}
			}
		}
	}
	setFloat := func(key string, dest *float64) {
		if v, ok := data[key]; ok {
			switch val := v.(type) {
			case float64:
				*dest = val
			case map[string]any:
				if inner, ok := val["value"]; ok {
					if n, ok := inner.(float64); ok {
						*dest = n
					}
				}
			}
		}
	}

	decodeInto := func(key string, dest any) {
		raw, ok := data[key]
		if !ok {
			return
		}
		blob, err := json.Marshal(raw)
		if err != nil {
			return
		}
		_ = json.Unmarshal(blob, dest)
	}

	setInt("NoKV.Stats.Entries", &snap.Entries)
	setInt("NoKV.Stats.Flush.Pending", &snap.Flush.Pending)
	setInt("NoKV.Stats.Compaction.Backlog", &snap.Compaction.Backlog)
	setFloat("NoKV.Stats.Compaction.MaxScore", &snap.Compaction.MaxScore)
	setFloat("NoKV.Stats.Cache.IndexHitRate", &snap.Cache.IndexHitRate)
	setFloat("NoKV.Stats.Cache.L0HitRate", &snap.Cache.BlockL0HitRate)
	setFloat("NoKV.Stats.Cache.L1HitRate", &snap.Cache.BlockL1HitRate)
	setFloat("NoKV.Stats.Cache.BloomHitRate", &snap.Cache.BloomHitRate)
	var intVal int64
	setInt("NoKV.Stats.Write.HotKeyLimited", &intVal)
	if intVal < 0 {
		intVal = 0
	}
	snap.Write.HotKeyLimited = uint64(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.Segments", &intVal)
	snap.ValueLog.Segments = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.PendingDeletes", &intVal)
	snap.ValueLog.PendingDeletes = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.DiscardQueue", &intVal)
	snap.ValueLog.DiscardQueue = int(intVal)
	setInt("NoKV.Stats.WAL.ActiveSegment", &snap.WAL.ActiveSegment)
	setInt("NoKV.Stats.WAL.Segments", &snap.WAL.SegmentCount)
	setInt("NoKV.Stats.WAL.ActiveSize", &snap.WAL.ActiveSize)
	intVal = 0
	setInt("NoKV.Stats.WAL.Removed", &intVal)
	if intVal > 0 {
		snap.WAL.SegmentsRemoved = uint64(intVal)
	}
	setFloat("NoKV.Stats.WAL.TypedRatio", &snap.WAL.TypedRecordRatio)
	intVal = 0
	setInt("NoKV.Stats.WAL.TypedWarning", &intVal)
	if intVal != 0 {
		snap.WAL.TypedRecordWarning = true
	}
	if v, ok := data["NoKV.Stats.WAL.TypedReason"]; ok {
		if raw, ok := v.(string); ok && raw != "" {
			if unq, err := strconv.Unquote(raw); err == nil {
				snap.WAL.TypedRecordReason = unq
			} else {
				snap.WAL.TypedRecordReason = raw
			}
		}
	}
	intVal = 0
	setInt("NoKV.Stats.WAL.AutoRuns", &intVal)
	if intVal > 0 {
		snap.WAL.AutoGCRuns = uint64(intVal)
	}
	intVal = 0
	setInt("NoKV.Stats.WAL.AutoRemoved", &intVal)
	if intVal > 0 {
		snap.WAL.AutoGCRemoved = uint64(intVal)
	}
	setInt("NoKV.Stats.WAL.AutoLastUnix", &snap.WAL.AutoGCLastUnix)
	intVal = 0
	setInt("NoKV.Stats.Raft.Groups", &intVal)
	snap.Raft.GroupCount = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.Raft.LaggingGroups", &intVal)
	snap.Raft.LaggingGroups = int(intVal)
	setInt("NoKV.Stats.Raft.MaxLagSegments", &snap.Raft.MaxLagSegments)
	intVal = 0
	setInt("NoKV.Stats.Raft.MinSegment", &intVal)
	snap.Raft.MinLogSegment = uint32(intVal)
	intVal = 0
	setInt("NoKV.Stats.Raft.MaxSegment", &intVal)
	snap.Raft.MaxLogSegment = uint32(intVal)
	intVal = 0
	setInt("NoKV.Stats.Raft.LagWarning", &intVal)
	if intVal != 0 {
		snap.Raft.LagWarning = true
	}
	setInt("NoKV.Stats.Raft.LagThreshold", &snap.Raft.LagWarnThreshold)
	setInt("NoKV.Stats.LSM.ValueBytes", &snap.LSM.ValueBytesTotal)
	setFloat("NoKV.Stats.Compaction.ValueWeight", &snap.Compaction.ValueWeight)
	setFloat("NoKV.Stats.LSM.ValueDensityMax", &snap.LSM.ValueDensityMax)
	intVal = 0
	setInt("NoKV.Stats.LSM.ValueDensityAlert", &intVal)
	if intVal != 0 {
		snap.LSM.ValueDensityAlert = true
	}
	setInt("NoKV.Stats.Region.Total", &snap.Region.Total)
	setInt("NoKV.Stats.Region.New", &snap.Region.New)
	setInt("NoKV.Stats.Region.Running", &snap.Region.Running)
	setInt("NoKV.Stats.Region.Removing", &snap.Region.Removing)
	setInt("NoKV.Stats.Region.Tombstone", &snap.Region.Tombstone)
	setInt("NoKV.Stats.Region.Other", &snap.Region.Other)
	setInt("NoKV.Txns.Active", &snap.Txn.Active)
	intVal = 0
	setInt("NoKV.Txns.Started", &intVal)
	snap.Txn.Started = uint64(intVal)
	intVal = 0
	setInt("NoKV.Txns.Committed", &intVal)
	snap.Txn.Committed = uint64(intVal)
	intVal = 0
	setInt("NoKV.Txns.Conflicts", &intVal)
	snap.Txn.Conflicts = uint64(intVal)
	setInt("NoKV.Stats.Write.QueueDepth", &snap.Write.QueueDepth)
	setInt("NoKV.Stats.Write.QueueEntries", &snap.Write.QueueEntries)
	setInt("NoKV.Stats.Write.QueueBytes", &snap.Write.QueueBytes)
	setFloat("NoKV.Stats.Write.BatchAvgEntries", &snap.Write.AvgBatchEntries)
	setFloat("NoKV.Stats.Write.BatchAvgBytes", &snap.Write.AvgBatchBytes)
	setFloat("NoKV.Stats.Write.RequestWaitMs", &snap.Write.AvgRequestWaitMs)
	setFloat("NoKV.Stats.Write.ValueLogMs", &snap.Write.AvgValueLogMs)
	setFloat("NoKV.Stats.Write.ApplyMs", &snap.Write.AvgApplyMs)
	setInt("NoKV.Stats.Write.Batches", &snap.Write.BatchesTotal)
	intVal = 0
	setInt("NoKV.Stats.Write.Throttle", &intVal)
	if intVal != 0 {
		snap.Write.ThrottleActive = true
	}
	if raw, ok := data["NoKV.Stats.HotKeys"]; ok {
		switch v := raw.(type) {
		case []any:
			for _, elem := range v {
				if kv, ok := elem.(map[string]any); ok {
					key, _ := kv["key"].(string)
					var count int32
					switch c := kv["count"].(type) {
					case float64:
						count = int32(c)
					case int64:
						count = int32(c)
					case map[string]any:
						if inner, ok := c["value"].(float64); ok {
							count = int32(inner)
						}
					}
					snap.Hot.ReadKeys = append(snap.Hot.ReadKeys, NoKV.HotKeyStat{Key: key, Count: count})
				}
			}
		case map[string]any:
			for key, val := range v {
				var count int32
				switch c := val.(type) {
				case float64:
					count = int32(c)
				case map[string]any:
					if inner, ok := c["value"].(float64); ok {
						count = int32(inner)
					}
				}
				snap.Hot.ReadKeys = append(snap.Hot.ReadKeys, NoKV.HotKeyStat{Key: key, Count: count})
			}
		}
	}
	if raw, ok := data["NoKV.Stats.HotRing"]; ok {
		if blob, err := json.Marshal(raw); err == nil {
			var hs hotring.Stats
			if err := json.Unmarshal(blob, &hs); err == nil {
				snap.Hot.ReadRing = &hs
			}
		}
	}
	if raw, ok := data["NoKV.Stats.HotWriteKeys"]; ok {
		switch v := raw.(type) {
		case []any:
			for _, elem := range v {
				if kv, ok := elem.(map[string]any); ok {
					key, _ := kv["key"].(string)
					var count int32
					switch c := kv["count"].(type) {
					case float64:
						count = int32(c)
					case int64:
						count = int32(c)
					case map[string]any:
						if inner, ok := c["value"].(float64); ok {
							count = int32(inner)
						}
					}
					snap.Hot.WriteKeys = append(snap.Hot.WriteKeys, NoKV.HotKeyStat{Key: key, Count: count})
				}
			}
		case map[string]any:
			for key, val := range v {
				var count int32
				switch c := val.(type) {
				case float64:
					count = int32(c)
				case map[string]any:
					if inner, ok := c["value"].(float64); ok {
						count = int32(inner)
					}
				}
				snap.Hot.WriteKeys = append(snap.Hot.WriteKeys, NoKV.HotKeyStat{Key: key, Count: count})
			}
		}
	}
	if raw, ok := data["NoKV.Stats.HotWriteRing"]; ok {
		if blob, err := json.Marshal(raw); err == nil {
			var hs hotring.Stats
			if err := json.Unmarshal(blob, &hs); err == nil {
				snap.Hot.WriteRing = &hs
			}
		}
	}
	if raw, ok := data["NoKV.Stats.LSM.Levels"]; ok {
		switch levels := raw.(type) {
		case []any:
			for _, elem := range levels {
				if m, ok := elem.(map[string]any); ok {
					var lvl NoKV.LSMLevelStats
					if v, ok := m["level"].(float64); ok {
						lvl.Level = int(v)
					}
					if v, ok := m["tables"].(float64); ok {
						lvl.TableCount = int(v)
					}
					if v, ok := m["size_bytes"].(float64); ok {
						lvl.SizeBytes = int64(v)
					}
					if v, ok := m["value_bytes"].(float64); ok {
						lvl.ValueBytes = int64(v)
					}
					if v, ok := m["stale_bytes"].(float64); ok {
						lvl.StaleBytes = int64(v)
					}
					if v, ok := m["ingest_tables"].(float64); ok {
						lvl.IngestTables = int(v)
					}
					if v, ok := m["ingest_size_bytes"].(float64); ok {
						lvl.IngestSizeBytes = int64(v)
					}
					if v, ok := m["ingest_value_bytes"].(float64); ok {
						lvl.IngestValueBytes = int64(v)
					}
					snap.LSM.Levels = append(snap.LSM.Levels, lvl)
				}
			}
		}
	}
	if raw, ok := data["NoKV.Stats.WAL.RecordCounts"]; ok {
		if blob, err := json.Marshal(raw); err == nil {
			var counts wal.RecordMetrics
			if err := json.Unmarshal(blob, &counts); err == nil {
				snap.WAL.RecordCounts = counts
			}
		}
	}
	decodeInto("NoKV.Stats.ValueLogGC", &snap.ValueLog.GC)
	decodeInto("NoKV.Stats.Transport", &snap.Transport)
	decodeInto("NoKV.Redis", &snap.Redis)
	return snap
}

func ensureManifestExists(workDir string) error {
	if _, err := stat(filepath.Join(workDir, "CURRENT")); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("manifest not found in %s", workDir)
		}
		return err
	}
	return nil
}

func fileIDs(files []manifest.FileMeta) []uint64 {
	out := make([]uint64, 0, len(files))
	for _, meta := range files {
		out = append(out, meta.FileID)
	}
	return out
}

func totalSize(files []manifest.FileMeta) uint64 {
	var total uint64
	for _, meta := range files {
		total += meta.Size
	}
	return total
}

func totalValue(files []manifest.FileMeta) uint64 {
	var total uint64
	for _, meta := range files {
		total += meta.ValueSize
	}
	return total
}

func firstRegionMetrics() *storepkg.RegionMetrics {
	for _, st := range storepkg.Stores() {
		if st == nil {
			continue
		}
		if metrics := st.RegionMetrics(); metrics != nil {
			return metrics
		}
	}
	return nil
}

func formatRegionState(state manifest.RegionState) string {
	switch state {
	case manifest.RegionStateNew:
		return "new"
	case manifest.RegionStateRunning:
		return "running"
	case manifest.RegionStateRemoving:
		return "removing"
	case manifest.RegionStateTombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown(%d)", state)
	}
}

func formatPeers(peers []manifest.PeerMeta) string {
	if len(peers) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(peers))
	for _, p := range peers {
		parts = append(parts, fmt.Sprintf("{store:%d peer:%d}", p.StoreID, p.PeerID))
	}
	return fmt.Sprintf("[%s]", strings.Join(parts, " "))
}
