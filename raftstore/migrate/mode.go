package migrate

import (
	"encoding/json"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
)

type stateFile struct {
	Mode     Mode   `json:"mode"`
	StoreID  uint64 `json:"store_id,omitempty"`
	RegionID uint64 `json:"region_id,omitempty"`
	PeerID   uint64 `json:"peer_id,omitempty"`
}

func readState(workDir string) (stateFile, error) {
	path := filepath.Join(workDir, ModeFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if stderrors.Is(err, os.ErrNotExist) {
			return stateFile{Mode: ModeStandalone}, nil
		}
		return stateFile{}, fmt.Errorf("read mode file: %w", err)
	}
	var state stateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return stateFile{}, fmt.Errorf("decode mode file: %w", err)
	}
	switch state.Mode {
	case ModeStandalone, ModePreparing, ModeSeeded, ModeCluster:
		return state, nil
	default:
		return stateFile{}, fmt.Errorf("unknown migration mode %q", state.Mode)
	}
}

func readMode(workDir string) (Mode, error) {
	state, err := readState(workDir)
	if err != nil {
		return "", err
	}
	return state.Mode, nil
}
