package region

import (
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// Snapshot is an external view of the tracked region metadata.
type Snapshot struct {
	Regions []localmeta.RegionMeta `json:"regions"`
}

// PeerHandle is a lightweight view of a peer registered with the store. It is
// designed for diagnostics and scheduling components so they can iterate over
// the cluster topology without touching the internal map directly.
type PeerHandle struct {
	ID     uint64
	Peer   *peer.Peer
	Region *localmeta.RegionMeta
}

// RuntimeStatus captures store-local runtime state for one region.
type RuntimeStatus struct {
	Meta         localmeta.RegionMeta
	Hosted       bool
	LocalPeerID  uint64
	LeaderPeerID uint64
	Leader       bool
	AppliedIndex uint64
	AppliedTerm  uint64
}
