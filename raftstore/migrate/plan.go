package migrate

import (
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
)

// PlanResult is the read-only migration preflight result for one workdir.
type PlanResult struct {
	WorkDir             string   `json:"workdir"`
	Mode                Mode     `json:"mode"`
	Eligible            bool     `json:"eligible"`
	Blockers            []string `json:"blockers,omitempty"`
	Warnings            []string `json:"warnings,omitempty"`
	Next                string   `json:"next,omitempty"`
	LocalCatalogRegions int      `json:"local_catalog_regions"`
}

// BuildPlan inspects one standalone workdir and reports whether it is eligible
// to become a single-store cluster seed.
func BuildPlan(workDir string) (PlanResult, error) {
	workDir = filepath.Clean(workDir)
	if workDir == "" || workDir == "." {
		return PlanResult{}, fmt.Errorf("migrate: workdir is required")
	}

	result := PlanResult{
		WorkDir:  workDir,
		Mode:     ModeStandalone,
		Eligible: true,
		Next:     "nokv migrate init",
	}

	mode, err := readMode(workDir)
	if err != nil {
		addBlocker(&result, err.Error())
	} else {
		result.Mode = mode
	}
	if result.Mode != ModeStandalone {
		addBlocker(&result, fmt.Sprintf("workdir is already in %q mode", result.Mode))
	}

	workDirExists := true
	var valueLogHeads map[uint32]manifest.ValueLogMeta
	if _, err := os.Stat(workDir); err != nil {
		if os.IsNotExist(err) {
			workDirExists = false
			addBlocker(&result, fmt.Sprintf("workdir not found: %s", workDir))
		} else {
			addBlocker(&result, fmt.Sprintf("stat workdir failed: %v", err))
		}
	}

	manifestPresent := false
	if err := ensureManifestPresent(workDir); err != nil {
		addBlocker(&result, err.Error())
	} else if err := manifest.Check(workDir, nil); err != nil {
		addBlocker(&result, fmt.Sprintf("manifest check failed: %v", err))
	} else {
		manifestPresent = true
		mgr, err := manifest.Open(workDir, nil)
		if err != nil {
			addBlocker(&result, fmt.Sprintf("manifest open failed: %v", err))
		} else {
			valueLogHeads = mgr.ValueLogHead()
			_ = mgr.Close()
		}
	}

	if err := wal.CheckDir(workDir, nil); err != nil {
		addBlocker(&result, fmt.Sprintf("wal check failed: %v", err))
	}

	opts := NoKV.NewDefaultOptions()
	opts.WorkDir = workDir
	vlogDir := filepath.Join(workDir, "vlog")
	bucketCount := max(opts.ValueLogBucketCount, 1)
	for bucket := range bucketCount {
		head, ok := valueLogHeads[uint32(bucket)]
		if !ok || !head.Valid {
			continue
		}
		cfg := vlogpkg.Config{
			Dir:      filepath.Join(vlogDir, fmt.Sprintf("bucket-%03d", bucket)),
			MaxSize:  int64(opts.ValueLogFileSize),
			Bucket:   uint32(bucket),
			FileMode: utils.DefaultFileMode,
		}
		if err := vlogpkg.CheckHead(cfg, head.FileID, uint32(head.Offset)); err != nil {
			if stderrors.Is(err, os.ErrNotExist) {
				continue
			}
			addBlocker(&result, fmt.Sprintf("vlog check failed for bucket %d: %v", bucket, err))
		}
	}

	if workDirExists && manifestPresent {
		localMeta, err := localmeta.OpenLocalStore(workDir, nil)
		if err != nil {
			addBlocker(&result, fmt.Sprintf("local catalog open failed: %v", err))
		} else {
			result.LocalCatalogRegions = len(localMeta.Snapshot())
			if result.LocalCatalogRegions > 0 {
				addBlocker(&result, fmt.Sprintf("local catalog already contains %d region(s)", result.LocalCatalogRegions))
			}
			_ = localMeta.Close()
		}
	}

	if result.Eligible {
		result.Next = "nokv migrate init"
	} else {
		result.Next = ""
	}
	return result, nil
}

func addBlocker(result *PlanResult, blocker string) {
	if result == nil || blocker == "" {
		return
	}
	result.Eligible = false
	result.Blockers = append(result.Blockers, blocker)
}

func ensureManifestPresent(workDir string) error {
	if _, err := os.Stat(filepath.Join(workDir, "CURRENT")); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("manifest not found in %s", workDir)
		}
		return err
	}
	return nil
}
