package migrate

import (
	"encoding/json"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/feichai0017/NoKV/engine/vfs"
)

const CheckpointFileName = "MIGRATION_PROGRESS.json"

type CheckpointStage string

const (
	CheckpointPreparingWritten CheckpointStage = "mode-preparing-written"
	CheckpointCatalogPersisted CheckpointStage = "local-catalog-persisted"
	CheckpointSeedExported     CheckpointStage = "seed-snapshot-exported"
	CheckpointRaftSeeded       CheckpointStage = "raft-seed-initialized"
	CheckpointSeededFinalized  CheckpointStage = "seeded-finalized"
	CheckpointExpandStarted    CheckpointStage = "expand-rollout-started"
	CheckpointExpandTarget     CheckpointStage = "expand-target-pending"
	CheckpointExpandHosted     CheckpointStage = "expand-target-hosted"
	CheckpointTransferStarted  CheckpointStage = "transfer-leader-started"
	CheckpointTransferReady    CheckpointStage = "transfer-leader-finished"
	CheckpointRemoveStarted    CheckpointStage = "remove-peer-started"
	CheckpointRemoveFinished   CheckpointStage = "remove-peer-finished"
)

type Checkpoint struct {
	Stage            CheckpointStage `json:"stage"`
	StoreID          uint64          `json:"store_id,omitempty"`
	RegionID         uint64          `json:"region_id,omitempty"`
	PeerID           uint64          `json:"peer_id,omitempty"`
	TargetStoreID    uint64          `json:"target_store_id,omitempty"`
	TargetPeerID     uint64          `json:"target_peer_id,omitempty"`
	CompletedTargets int             `json:"completed_targets,omitempty"`
	TotalTargets     int             `json:"total_targets,omitempty"`
	UpdatedAt        string          `json:"updated_at"`
}

func checkpointPath(workDir string) string {
	return filepath.Join(filepath.Clean(workDir), CheckpointFileName)
}

func readCheckpoint(workDir string) (*Checkpoint, error) {
	path := checkpointPath(workDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if stderrors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read migration checkpoint: %w", err)
	}
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode migration checkpoint: %w", err)
	}
	return &checkpoint, nil
}

func writeCheckpoint(workDir string, checkpoint Checkpoint) error {
	workDir = filepath.Clean(workDir)
	if workDir == "" || workDir == "." {
		return fmt.Errorf("write migration checkpoint: workdir is required")
	}
	if checkpoint.Stage == "" {
		return fmt.Errorf("write migration checkpoint: stage is required")
	}
	checkpoint.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	fs := vfs.Ensure(nil)
	path := checkpointPath(workDir)
	tmp := path + ".tmp"
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("encode migration checkpoint: %w", err)
	}
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open migration checkpoint temp file: %w", err)
	}
	writeErr := writeCheckpointAll(f, payload)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = fs.Remove(tmp)
		return fmt.Errorf("write migration checkpoint temp file: %w", writeErr)
	}
	if syncErr != nil {
		_ = fs.Remove(tmp)
		return fmt.Errorf("sync migration checkpoint temp file: %w", syncErr)
	}
	if closeErr != nil {
		_ = fs.Remove(tmp)
		return fmt.Errorf("close migration checkpoint temp file: %w", closeErr)
	}
	if err := fs.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename migration checkpoint file: %w", err)
	}
	if err := vfs.SyncDir(fs, workDir); err != nil {
		return fmt.Errorf("sync migration checkpoint dir: %w", err)
	}
	return nil
}

func writeCheckpointAll(f interface{ Write([]byte) (int, error) }, data []byte) error {
	for len(data) > 0 {
		n, err := f.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func resumeHint(mode Mode, checkpoint *Checkpoint) string {
	if checkpoint == nil {
		return ""
	}
	switch mode {
	case ModePreparing:
		switch checkpoint.Stage {
		case CheckpointPreparingWritten:
			return "re-run nokv migrate init to continue after the mode gate was written"
		case CheckpointCatalogPersisted:
			return "re-run nokv migrate init to continue after local catalog persistence"
		case CheckpointSeedExported:
			return "re-run nokv migrate init to continue after seed snapshot export"
		case CheckpointRaftSeeded:
			return "re-run nokv migrate init to finalize seeded mode"
		}
	case ModeSeeded:
		if checkpoint.Stage == CheckpointSeededFinalized {
			return "local standalone-to-seed promotion already completed"
		}
		fallthrough
	case ModeCluster:
		if checkpoint.Stage == CheckpointSeededFinalized {
			return "local standalone-to-seed promotion already completed"
		}
		switch checkpoint.Stage {
		case CheckpointExpandStarted:
			return "re-run nokv migrate expand to continue the recorded rollout"
		case CheckpointExpandTarget:
			return fmt.Sprintf("re-run nokv migrate expand to continue peer rollout toward store=%d peer=%d", checkpoint.TargetStoreID, checkpoint.TargetPeerID)
		case CheckpointExpandHosted:
			return fmt.Sprintf("expand rollout has completed %d/%d target(s); continue with nokv migrate expand or the next membership step", checkpoint.CompletedTargets, checkpoint.TotalTargets)
		case CheckpointTransferStarted:
			return fmt.Sprintf("re-run nokv migrate transfer-leader to continue leadership movement toward peer=%d", checkpoint.TargetPeerID)
		case CheckpointTransferReady:
			return fmt.Sprintf("leader transfer toward peer=%d completed; continue with the next membership step", checkpoint.TargetPeerID)
		case CheckpointRemoveStarted:
			return fmt.Sprintf("re-run nokv migrate remove-peer to continue removal of peer=%d", checkpoint.TargetPeerID)
		case CheckpointRemoveFinished:
			return fmt.Sprintf("peer removal for peer=%d completed; continue with the next migration step", checkpoint.TargetPeerID)
		}
	}
	return ""
}
