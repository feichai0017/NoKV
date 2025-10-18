package engine

import (
	"errors"
	"fmt"
	"os"

	myraft "github.com/feichai0017/NoKV/raft"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

// ExportSnapshot persists the current raft snapshot from storage to the supplied file path.
// If the snapshot is empty the file is removed (best-effort) and the function returns without error.
func ExportSnapshot(storage PeerStorage, path string) error {
	if storage == nil {
		return fmt.Errorf("engine: snapshot export requires storage")
	}
	snap, err := storage.Snapshot()
	if err != nil {
		return err
	}
	if myraft.IsEmptySnap(snap) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("engine: remove empty snapshot %s: %w", path, err)
		}
		return nil
	}
	pbSnap := raftpb.Snapshot(snap)
	data, err := (&pbSnap).Marshal()
	if err != nil {
		return fmt.Errorf("engine: marshal snapshot: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("engine: write snapshot %s: %w", path, err)
	}
	return nil
}

// ImportSnapshot loads a raft snapshot from the provided file and applies it to storage.
// Missing files are treated as no-op (no snapshot to import).
func ImportSnapshot(storage PeerStorage, path string) error {
	if storage == nil {
		return fmt.Errorf("engine: snapshot import requires storage")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("engine: read snapshot %s: %w", path, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("engine: snapshot file %s is empty", path)
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return fmt.Errorf("engine: unmarshal snapshot %s: %w", path, err)
	}
	return storage.ApplySnapshot(myraft.Snapshot(snap))
}
