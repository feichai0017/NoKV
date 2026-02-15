package peer

import (
	"path/filepath"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
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
	StorageDir       string
	WAL              *wal.Manager
	Manifest         *manifest.Manager
	GroupID          uint64
	Region           *manifest.RegionMeta
	LogRetainEntries uint64
	MaxInFlightApply uint64
}

// ResolveStorage chooses the backing log engine (in-memory, on-disk, or WAL).
func ResolveStorage(cfg *Config) (engine.PeerStorage, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.WAL != nil && cfg.Manifest != nil {
		return engine.OpenWALStorage(engine.WALStorageConfig{
			GroupID:  nonZeroGroupID(cfg.GroupID),
			WAL:      cfg.WAL,
			Manifest: cfg.Manifest,
		})
	}
	if cfg.WAL != nil || cfg.Manifest != nil {
		return nil, utils.ErrMissingManifestOrWAL
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
