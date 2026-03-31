package migrate

import (
	"fmt"
	"os"
	"path/filepath"

	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/vfs"
)

// StatusResult describes the current migration mode of one workdir.
type StatusResult struct {
	WorkDir             string   `json:"workdir"`
	Mode                Mode     `json:"mode"`
	StoreID             uint64   `json:"store_id,omitempty"`
	RegionID            uint64   `json:"region_id,omitempty"`
	PeerID              uint64   `json:"peer_id,omitempty"`
	LocalCatalogRegions int      `json:"local_catalog_regions,omitempty"`
	SeedSnapshotDir     string   `json:"seed_snapshot_dir,omitempty"`
	SeedSnapshotPresent bool     `json:"seed_snapshot_present,omitempty"`
	Next                string   `json:"next,omitempty"`
	Warnings            []string `json:"warnings,omitempty"`
}

// ReadStatus returns the current migration mode and seed identifiers for one
// workdir.
func ReadStatus(workDir string) (StatusResult, error) {
	workDir = filepath.Clean(workDir)
	if workDir == "" || workDir == "." {
		return StatusResult{}, fmt.Errorf("migrate: workdir is required")
	}
	state, err := readState(workDir)
	if err != nil {
		return StatusResult{}, err
	}
	result := StatusResult{
		WorkDir:  workDir,
		Mode:     state.Mode,
		StoreID:  state.StoreID,
		RegionID: state.RegionID,
		PeerID:   state.PeerID,
	}

	localMeta, err := raftmeta.OpenLocalStore(workDir, nil)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("local catalog open failed: %v", err))
	} else {
		result.LocalCatalogRegions = len(localMeta.Snapshot())
		_ = localMeta.Close()
	}

	if result.RegionID != 0 {
		result.SeedSnapshotDir = SeedSnapshotDir(workDir, result.RegionID)
		fs := vfs.Ensure(nil)
		if _, err := fs.Stat(result.SeedSnapshotDir); err == nil {
			result.SeedSnapshotPresent = true
		} else if !os.IsNotExist(err) {
			result.Warnings = append(result.Warnings, fmt.Sprintf("seed snapshot stat failed: %v", err))
		}
	}

	switch result.Mode {
	case ModeStandalone:
		result.Next = "nokv migrate plan"
	case ModePreparing:
		result.Next = "retry nokv migrate init after inspecting partial migration state"
	case ModeSeeded:
		result.Next = fmt.Sprintf("nokv serve --workdir %s --store-id %d --pd-addr <pd>", result.WorkDir, result.StoreID)
	case ModeCluster:
		result.Next = "nokv migrate expand | nokv migrate transfer-leader | nokv migrate remove-peer"
	}

	return result, nil
}
