package engine

import myraft "github.com/feichai0017/NoKV/raft"

// PeerStorage is the persistence contract for a raft peer. It embeds the
// etcd/raft Storage interface and extends it with helpers to append entries,
// apply snapshots, and update hard state. Additional conveniences (e.g.
// MaybeCompact) are exposed by concrete implementations when needed.
type PeerStorage interface {
	myraft.Storage
	Append(entries []myraft.Entry) error
	ApplySnapshot(snap myraft.Snapshot) error
	SetHardState(st myraft.HardState) error
}
