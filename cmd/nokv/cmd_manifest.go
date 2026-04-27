package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/feichai0017/NoKV/engine/manifest"
)

var stat = os.Stat

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

	mgr, err := manifest.Open(*workDir, nil)
	if err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()

	version := mgr.Current()
	out := map[string]any{}
	heads := mgr.ValueLogHead()
	if len(heads) > 0 {
		buckets := make([]uint32, 0, len(heads))
		for bucket := range heads {
			buckets = append(buckets, bucket)
		}
		slices.Sort(buckets)
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

	if heads != nil {
		buckets := make([]uint32, 0, len(heads))
		for bucket := range heads {
			buckets = append(buckets, bucket)
		}
		slices.Sort(buckets)
		for _, bucket := range buckets {
			meta := heads[bucket]
			_, _ = fmt.Fprintf(w, "ValueLog Head[%d]     : fid=%d offset=%d valid=%v\n", bucket, meta.FileID, meta.Offset, meta.Valid)
		}
	}
	_, _ = fmt.Fprintln(w, "Levels:")
	for _, lvl := range levelInfo {
		_, _ = fmt.Fprintf(w, "  - L%d files=%d total=%d bytes value=%d bytes ids=%v\n",
			lvl["level"], lvl["file_count"], lvl["total_bytes"], lvl["value_bytes"], lvl["file_ids"])
	}
	_, _ = fmt.Fprintln(w, "ValueLog segments:")
	for _, vl := range valueLogs {
		_, _ = fmt.Fprintf(w, "  - bucket=%d fid=%d offset=%d valid=%v\n", vl["bucket"], vl["fid"], vl["offset"], vl["valid"])
	}
	return nil
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
