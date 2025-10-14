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

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
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

Run "nokv <command> -h" for command-specific flags.`)
}

func runStatsCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory (offline snapshot)")
	expvarURL := fs.String("expvar", "", "HTTP endpoint exposing /debug/vars (overrides workdir)")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
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
		snap, err = localStatsSnapshot(*workDir)
	default:
		return fmt.Errorf("either --workdir or --expvar must be specified")
	}
	if err != nil {
		return err
	}

	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(snap)
	}

	fmt.Fprintf(w, "Flush.Pending          %d\n", snap.FlushPending)
	fmt.Fprintf(w, "Compaction.Backlog     %d\n", snap.CompactionBacklog)
	fmt.Fprintf(w, "Compaction.MaxScore    %.2f\n", snap.CompactionMaxScore)
	fmt.Fprintf(w, "ValueLog.Segments      %d\n", snap.ValueLogSegments)
	fmt.Fprintf(w, "ValueLog.PendingDelete %d\n", snap.ValueLogPendingDel)
	fmt.Fprintf(w, "ValueLog.DiscardQueue  %d\n", snap.ValueLogDiscardQueue)
	if !snap.ValueLogHead.IsZero() {
		fmt.Fprintf(w, "ValueLog.Head          fid=%d offset=%d len=%d\n",
			snap.ValueLogHead.Fid, snap.ValueLogHead.Offset, snap.ValueLogHead.Len)
	}
	fmt.Fprintf(w, "Txns.Active            %d\n", snap.TxnsActive)
	fmt.Fprintf(w, "Txns.StartedTotal      %d\n", snap.TxnsStarted)
	fmt.Fprintf(w, "Txns.CommittedTotal    %d\n", snap.TxnsCommitted)
	fmt.Fprintf(w, "Txns.ConflictsTotal    %d\n", snap.TxnsConflicts)
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
		levelInfo = append(levelInfo, map[string]any{
			"level":       level,
			"file_count":  len(files),
			"file_ids":    fileIDs(files),
			"total_bytes": totalSize(files),
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
		fmt.Fprintf(w, "  - L%d files=%d total=%d bytes ids=%v\n",
			lvl["level"], lvl["file_count"], lvl["total_bytes"], lvl["file_ids"])
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

func localStatsSnapshot(workDir string) (NoKV.StatsSnapshot, error) {
	if workDir == "" {
		return NoKV.StatsSnapshot{}, fmt.Errorf("workdir is required")
	}
	opts := NoKV.NewDefaultOptions()
	opts.WorkDir = workDir
	db := NoKV.Open(opts)
	defer func() {
		_ = db.Close()
	}()
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

	setInt("NoKV.Stats.Flush.Pending", &snap.FlushPending)
	setInt("NoKV.Stats.Compaction.Backlog", &snap.CompactionBacklog)
	setFloat("NoKV.Stats.Compaction.MaxScore", &snap.CompactionMaxScore)
	var intVal int64
	setInt("NoKV.Stats.ValueLog.Segments", &intVal)
	snap.ValueLogSegments = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.PendingDeletes", &intVal)
	snap.ValueLogPendingDel = int(intVal)
	intVal = 0
	setInt("NoKV.Stats.ValueLog.DiscardQueue", &intVal)
	snap.ValueLogDiscardQueue = int(intVal)
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
