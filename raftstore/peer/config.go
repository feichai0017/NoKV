package peer

import (
	"path/filepath"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/transport"
)

// defaultLogRetainEntries defines the number of recent entries a peer keeps
// un-compacted even after they are applied.
const defaultLogRetainEntries = 4096

// Config captures everything required to bootstrap a peer.
type Config struct {
	RaftConfig       myraft.Config
	Peers            []myraft.Peer
	Transport        transport.Transport
	Apply            ApplyFunc
	AdminApply       AdminApplyFunc
	ConfChange       ConfChangeHandler
	SnapshotExport   SnapshotExportFunc
	SnapshotApply    SnapshotApplyFunc
	Storage          engine.PeerStorage
	StorageDir       string
	GroupID          uint64
	Region           *raftmeta.RegionMeta
	LogRetainEntries uint64
	MaxInFlightApply uint64
	// AllowSnapshotInstallRetry permits snapshot payload install onto durable
	// state that was previously written by an unpublished install attempt. It is
	// only intended for store-local install-before-publish retry paths.
	AllowSnapshotInstallRetry bool
}

// ResolveStorage chooses the backing log engine (in-memory, on-disk, or WAL).
func ResolveStorage(cfg *Config) (engine.PeerStorage, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.Storage != nil {
		return cfg.Storage, nil
	}
	if cfg.StorageDir != "" {
		return engine.OpenDiskStorage(filepath.Clean(cfg.StorageDir), nil)
	}
	return myraft.NewMemoryStorage(), nil
}

func nonZeroGroupID(id uint64) uint64 {
	if id == 0 {
		return 1
	}
	return id
}
