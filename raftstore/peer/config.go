package peer

import (
	"path/filepath"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
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
	Storage          raftlog.PeerStorage
	StorageDir       string
	GroupID          uint64
	Region           *localmeta.RegionMeta
	LogRetainEntries uint64
	MaxInFlightApply uint64
	// AllowSnapshotInstallRetry permits snapshot payload install onto durable
	// state that was previously written by an unpublished install attempt. It is
	// only intended for store-local install-before-publish retry paths.
	AllowSnapshotInstallRetry bool
	// FastLeaseRead requires the raft group to use etcd/raft lease-based
	// ReadIndex. Reads still enter RawNode.ReadIndex; quorum freshness remains
	// owned by raft instead of a store-local shortcut.
	FastLeaseRead bool
	// BatchMaxSize is the number of proposals collected before flushing
	// as a single Ready cycle. Defaults to 64 when zero.
	BatchMaxSize int
	// BatchMaxWait is the maximum time the batcher waits before flushing
	// a non-full batch. Defaults to 1ms when zero.
	BatchMaxWait time.Duration
}

// EnableLeaseRead configures etcd/raft's lease-based ReadIndex path. Reads
// still flow through RawNode.ReadIndex and wait for the returned committed
// index to be applied, but the leader can answer from its local lease instead
// of broadcasting every read to a quorum. CheckQuorum is required by
// etcd/raft for this mode: it is the fence that makes a partitioned old leader
// step down once its quorum lease is no longer defensible.
func EnableLeaseRead(cfg myraft.Config) myraft.Config {
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = myraft.ReadOnlyLeaseBased
	return cfg
}

// ResolveStorage chooses the backing log engine (in-memory, on-disk, or WAL).
func ResolveStorage(cfg *Config) (raftlog.PeerStorage, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.Storage != nil {
		return cfg.Storage, nil
	}
	if cfg.StorageDir != "" {
		return raftlog.OpenDiskStorage(filepath.Clean(cfg.StorageDir), nil)
	}
	return myraft.NewMemoryStorage(), nil
}

func nonZeroGroupID(id uint64) uint64 {
	if id == 0 {
		return 1
	}
	return id
}
