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
	"strings"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
)

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stdout)
		os.Exit(1)
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
		os.Exit(1)
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
	fmt.Fprintf(w, "Flush.Pending          %d\n", snap.FlushPending)
	fmt.Fprintf(w, "Compaction.Backlog     %d\n", snap.CompactionBacklog)
	fmt.Fprintf(w, "Compaction.MaxScore    %.2f\n", snap.CompactionMaxScore)
	fmt.Fprintf(w, "Flush.Wait.LastMs      %.2f\n", snap.FlushLastWaitMs)
	fmt.Fprintf(w, "Flush.Wait.MaxMs       %.2f\n", snap.FlushMaxWaitMs)
	fmt.Fprintf(w, "Flush.Build.LastMs     %.2f\n", snap.FlushLastBuildMs)
	fmt.Fprintf(w, "Flush.Build.MaxMs      %.2f\n", snap.FlushMaxBuildMs)
	fmt.Fprintf(w, "Flush.Release.LastMs   %.2f\n", snap.FlushLastReleaseMs)
	fmt.Fprintf(w, "Flush.Release.MaxMs    %.2f\n", snap.FlushMaxReleaseMs)
	fmt.Fprintf(w, "Compaction.LastMs      %.2f\n", snap.CompactionLastDurationMs)
	fmt.Fprintf(w, "Compaction.MaxMs       %.2f\n", snap.CompactionMaxDurationMs)
	fmt.Fprintf(w, "Compaction.Runs        %d\n", snap.CompactionRuns)
	fmt.Fprintf(w, "ValueLog.Segments      %d\n", snap.ValueLogSegments)
	fmt.Fprintf(w, "ValueLog.PendingDelete %d\n", snap.ValueLogPendingDel)
	fmt.Fprintf(w, "ValueLog.DiscardQueue  %d\n", snap.ValueLogDiscardQueue)
	if !snap.ValueLogHead.IsZero() {
		fmt.Fprintf(w, "ValueLog.Head          fid=%d offset=%d len=%d\n",
			snap.ValueLogHead.Fid, snap.ValueLogHead.Offset, snap.ValueLogHead.Len)
	}
	fmt.Fprintf(w, "Write.HotKeyThrottled  %d\n", snap.HotWriteLimited)
	fmt.Fprintf(w, "Compaction.ValueWeight %.2f", snap.CompactionValueWeight)
	if snap.CompactionValueWeightSuggested > snap.CompactionValueWeight {
		fmt.Fprintf(w, " (suggested %.2f)", snap.CompactionValueWeightSuggested)
	}
	fmt.Fprintln(w)
	if snap.LSMValueDensityMax > 0 {
		fmt.Fprintf(w, "LSM.ValueDensityMax    %.2f\n", snap.LSMValueDensityMax)
	}
	if snap.LSMValueDensityAlert {
		fmt.Fprintln(w, "LSM.ValueDensityAlert  true")
	}
	fmt.Fprintf(w, "WAL.ActiveSegment      %d (segments=%d removed=%d)\n", snap.WALActiveSegment, snap.WALSegmentCount, snap.WALSegmentsRemoved)
	fmt.Fprintf(w, "WAL.ActiveSize         %d bytes\n", snap.WALActiveSize)
	if snap.WALRecordCounts.Total() > 0 {
		r := snap.WALRecordCounts
		fmt.Fprintf(w, "WAL.Records            entries=%d raft_entries=%d raft_states=%d raft_snapshots=%d other=%d\n",
			r.Entries, r.RaftEntries, r.RaftStates, r.RaftSnapshots, r.Other)
	}
	fmt.Fprintf(w, "WAL.RaftSegments       %d (removable=%d)\n", snap.WALSegmentsWithRaftRecords, snap.WALRemovableRaftSegments)
	if snap.WALTypedRecordRatio > 0 || snap.WALTypedRecordWarning {
		fmt.Fprintf(w, "WAL.TypedRatio         %.2f\n", snap.WALTypedRecordRatio)
	}
	if snap.WALTypedRecordWarning && snap.WALTypedRecordReason != "" {
		fmt.Fprintf(w, "WAL.Warning            %s\n", snap.WALTypedRecordReason)
	}
	if snap.WALAutoGCRuns > 0 || snap.WALAutoGCRemoved > 0 || snap.WALAutoGCLastUnix > 0 {
		last := "never"
		if snap.WALAutoGCLastUnix > 0 {
			last = time.Unix(snap.WALAutoGCLastUnix, 0).Format(time.RFC3339)
		}
		fmt.Fprintf(w, "WAL.AutoGC             runs=%d removed=%d last=%s\n", snap.WALAutoGCRuns, snap.WALAutoGCRemoved, last)
	}
	if snap.RaftGroupCount > 0 {
		fmt.Fprintf(w, "Raft.Groups            %d lagging=%d maxLagSegments=%d\n",
			snap.RaftGroupCount, snap.RaftLaggingGroups, snap.RaftMaxLagSegments)
		fmt.Fprintf(w, "Raft.SegmentRange      min=%d max=%d\n", snap.RaftMinLogSegment, snap.RaftMaxLogSegment)
		if snap.RaftLagWarnThreshold > 0 {
			fmt.Fprintf(w, "Raft.LagThreshold      %d segments\n", snap.RaftLagWarnThreshold)
		}
		if snap.RaftLagWarning {
			fmt.Fprintf(w, "Raft.Warning           lagging=%d maxLag=%d (threshold=%d)\n",
				snap.RaftLaggingGroups, snap.RaftMaxLagSegments, snap.RaftLagWarnThreshold)
		}
	}
	fmt.Fprintf(w, "Txns.Active            %d\n", snap.TxnsActive)
	fmt.Fprintf(w, "Txns.StartedTotal      %d\n", snap.TxnsStarted)
	fmt.Fprintf(w, "Txns.CommittedTotal    %d\n", snap.TxnsCommitted)
	fmt.Fprintf(w, "Txns.ConflictsTotal    %d\n", snap.TxnsConflicts)
	fmt.Fprintf(w, "Regions.Total          %d (new=%d running=%d removing=%d tombstone=%d other=%d)\n",
		snap.RegionTotal, snap.RegionNew, snap.RegionRunning, snap.RegionRemoving, snap.RegionTombstone, snap.RegionOther)
	if snap.LSMValueBytesTotal > 0 {
		fmt.Fprintf(w, "LSM.ValueBytesTotal   %d\n", snap.LSMValueBytesTotal)
	}
	if len(snap.LSMLevels) > 0 {
		fmt.Fprintln(w, "LSM.Levels:")
		for _, lvl := range snap.LSMLevels {
			fmt.Fprintf(w, "  - L%d tables=%d size=%dB value=%dB stale=%dB",
				lvl.Level, lvl.TableCount, lvl.SizeBytes, lvl.ValueBytes, lvl.StaleBytes)
			if lvl.IngestTables > 0 {
				fmt.Fprintf(w, " ingestTables=%d ingestSize=%dB ingestValue=%dB",
					lvl.IngestTables, lvl.IngestSizeBytes, lvl.IngestValueBytes)
			}
			fmt.Fprintln(w)
		}
	}
	if len(snap.ColumnFamilies) > 0 {
		fmt.Fprintln(w, "ColumnFamilies:")
		var names []string
		for name := range snap.ColumnFamilies {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			cf := snap.ColumnFamilies[name]
			fmt.Fprintf(w, "  - %s: reads=%d writes=%d\n", name, cf.Reads, cf.Writes)
		}
	}
	if len(snap.HotKeys) > 0 {
		fmt.Fprintln(w, "HotKeys:")
		for _, hk := range snap.HotKeys {
			fmt.Fprintf(w, "  - key=%q count=%d\n", hk.Key, hk.Count)
		}
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
	head := mgr.ValueLogHead()

	out := map[string]any{
		"log_pointer": map[string]any{
			"segment": version.LogSegment,
			"offset":  version.LogOffset,
		},
		"value_log_head": map[string]any{
			"fid":    head.FileID,
			"offset": head.Offset,
			"valid":  head.Valid,
		},
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
	var fids []uint32
	for fid := range version.ValueLogs {
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })
	for _, fid := range fids {
		meta := version.ValueLogs[fid]
		valueLogs = append(valueLogs, map[string]any{
			"fid":    fid,
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
	fmt.Fprintf(w, "ValueLog Head        : fid=%d offset=%d valid=%v\n", head.FileID, head.Offset, head.Valid)
	fmt.Fprintln(w, "Levels:")
	for _, lvl := range levelInfo {
		fmt.Fprintf(w, "  - L%d files=%d total=%d bytes value=%d bytes ids=%v\n",
			lvl["level"], lvl["file_count"], lvl["total_bytes"], lvl["value_bytes"], lvl["file_ids"])
	}
	fmt.Fprintln(w, "ValueLog segments:")
	for _, vl := range valueLogs {
		fmt.Fprintf(w, "  - fid=%d offset=%d valid=%v\n", vl["fid"], vl["offset"], vl["valid"])
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

	setInt("NoKV.Stats.Entries", &snap.Entries)
	setInt("NoKV.Stats.Flush.Pending", &snap.FlushPending)
	setInt("NoKV.Stats.Compaction.Backlog", &snap.CompactionBacklog)
	setFloat("NoKV.Stats.Compaction.MaxScore", &snap.CompactionMaxScore)
	var intVal int64
	setInt("NoKV.Stats.Write.HotKeyLimited", &intVal)
	if intVal < 0 {
		intVal = 0
	}
	snap.HotWriteLimited = uint64(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.Segments", &intVal)
	snap.ValueLogSegments = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.PendingDeletes", &intVal)
	snap.ValueLogPendingDel = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.DiscardQueue", &intVal)
	snap.ValueLogDiscardQueue = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.Raft.Groups", &intVal)
	snap.RaftGroupCount = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.Raft.LaggingGroups", &intVal)
	snap.RaftLaggingGroups = int(intVal)
	setInt("NoKV.Stats.Raft.MaxLagSegments", &snap.RaftMaxLagSegments)
	intVal = 0
	setInt("NoKV.Stats.Raft.MinSegment", &intVal)
	snap.RaftMinLogSegment = uint32(intVal)
	intVal = 0
	setInt("NoKV.Stats.Raft.MaxSegment", &intVal)
	snap.RaftMaxLogSegment = uint32(intVal)
	setInt("NoKV.Stats.LSM.ValueBytes", &snap.LSMValueBytesTotal)
	setFloat("NoKV.Stats.Compaction.ValueWeight", &snap.CompactionValueWeight)
	setFloat("NoKV.Stats.LSM.ValueDensityMax", &snap.LSMValueDensityMax)
	intVal = 0
	setInt("NoKV.Stats.LSM.ValueDensityAlert", &intVal)
	if intVal != 0 {
		snap.LSMValueDensityAlert = true
	}
	setInt("NoKV.Stats.Region.Total", &snap.RegionTotal)
	setInt("NoKV.Stats.Region.New", &snap.RegionNew)
	setInt("NoKV.Stats.Region.Running", &snap.RegionRunning)
	setInt("NoKV.Stats.Region.Removing", &snap.RegionRemoving)
	setInt("NoKV.Stats.Region.Tombstone", &snap.RegionTombstone)
	setInt("NoKV.Stats.Region.Other", &snap.RegionOther)
	setInt("NoKV.Txns.Active", &snap.TxnsActive)
	intVal = 0
	setInt("NoKV.Txns.Started", &intVal)
	snap.TxnsStarted = uint64(intVal)
	intVal = 0
	setInt("NoKV.Txns.Committed", &intVal)
	snap.TxnsCommitted = uint64(intVal)
	intVal = 0
	setInt("NoKV.Txns.Conflicts", &intVal)
	snap.TxnsConflicts = uint64(intVal)
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
					snap.HotKeys = append(snap.HotKeys, NoKV.HotKeyStat{Key: key, Count: count})
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
				snap.HotKeys = append(snap.HotKeys, NoKV.HotKeyStat{Key: key, Count: count})
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
					snap.LSMLevels = append(snap.LSMLevels, lvl)
				}
			}
		}
	}
	return snap
}

func ensureManifestExists(workDir string) error {
	if _, err := os.Stat(filepath.Join(workDir, "CURRENT")); err != nil {
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
