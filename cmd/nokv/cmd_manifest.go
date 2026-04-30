package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	if *asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	_, _ = fmt.Fprintln(w, "Levels:")
	for _, lvl := range levelInfo {
		_, _ = fmt.Fprintf(w, "  - L%d files=%d total=%d bytes value=%d bytes ids=%v\n",
			lvl["level"], lvl["file_count"], lvl["total_bytes"], lvl["value_bytes"], lvl["file_ids"])
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
