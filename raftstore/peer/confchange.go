package peer

import (
	"github.com/feichai0017/NoKV/manifest"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// ConfChangeEvent captures context for a configuration change entry that has
// been committed and applied by the raft peer.
type ConfChangeEvent struct {
	Peer       *Peer
	RegionMeta *manifest.RegionMeta
	ConfChange raftpb.ConfChangeV2
	Index      uint64
	Term       uint64
}

// ConfChangeHandler is invoked whenever a configuration change entry is
// applied. Returning an error aborts Ready processing so callers can surface
// failures (for example manifest persistence errors) to the raftstore.
type ConfChangeHandler func(ConfChangeEvent) error
