package migrate

import (
	"fmt"
	"path/filepath"
)

// StatusResult describes the current migration mode of one workdir.
type StatusResult struct {
	WorkDir  string `json:"workdir"`
	Mode     Mode   `json:"mode"`
	StoreID  uint64 `json:"store_id,omitempty"`
	RegionID uint64 `json:"region_id,omitempty"`
	PeerID   uint64 `json:"peer_id,omitempty"`
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
	return StatusResult{
		WorkDir:  workDir,
		Mode:     state.Mode,
		StoreID:  state.StoreID,
		RegionID: state.RegionID,
		PeerID:   state.PeerID,
	}, nil
}
