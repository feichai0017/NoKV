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

	vlogpkg "github.com/feichai0017/NoKV/engine/vlog"
)

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
		defer func() { _ = manager.Close() }()

		head := manager.Head()
		fids := manager.ListFIDs()
		slices.Sort(fids)

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

		_, _ = fmt.Fprintf(w, "Active FID : %d\n", manager.ActiveFID())
		_, _ = fmt.Fprintf(w, "Head       : fid=%d offset=%d\n", head.Fid, head.Offset)
		_, _ = fmt.Fprintf(w, "Segments   : %v\n", fids)
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
		slices.Sort(fids)
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
		_, _ = fmt.Fprintf(w, "Bucket %d\n", info["bucket"])
		_, _ = fmt.Fprintf(w, "  Active FID : %d\n", info["active_fid"])
		head := info["head"].(map[string]any)
		_, _ = fmt.Fprintf(w, "  Head       : fid=%d offset=%d\n", head["fid"], head["offset"])
		_, _ = fmt.Fprintf(w, "  Segments   : %v\n", info["segments"])
	}
	return nil
}
