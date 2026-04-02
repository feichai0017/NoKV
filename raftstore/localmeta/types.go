package localmeta

import (
	"maps"

	metaregion "github.com/feichai0017/NoKV/meta/region"
)

// Keep the store-local API stable while moving the underlying primitive region
// types into a neutral metadata package shared by localmeta and descriptor.
type PeerMeta = metaregion.Peer
type RegionState = metaregion.ReplicaState
type RegionEpoch = metaregion.Epoch

const (
	RegionStateNew       RegionState = metaregion.ReplicaStateNew
	RegionStateRunning   RegionState = metaregion.ReplicaStateRunning
	RegionStateRemoving  RegionState = metaregion.ReplicaStateRemoving
	RegionStateTombstone RegionState = metaregion.ReplicaStateTombstone
)

// RegionMeta captures the store-local runtime and recovery shape of one region.
//
// It belongs to raftstore local state, peer lifecycle, and restart recovery.
// It is intentionally smaller than the distributed topology descriptor and must
// not become control-plane authority.
type RegionMeta struct {
	ID       uint64      `json:"id"`
	StartKey []byte      `json:"start_key,omitempty"`
	EndKey   []byte      `json:"end_key,omitempty"`
	Epoch    RegionEpoch `json:"epoch"`
	Peers    []PeerMeta  `json:"peers,omitempty"`
	State    RegionState `json:"state"`
}

// CloneRegionMeta returns a deep copy of the provided region metadata.
func CloneRegionMeta(meta RegionMeta) RegionMeta {
	cp := meta
	if meta.StartKey != nil {
		cp.StartKey = append([]byte(nil), meta.StartKey...)
	}
	if meta.EndKey != nil {
		cp.EndKey = append([]byte(nil), meta.EndKey...)
	}
	if len(meta.Peers) > 0 {
		cp.Peers = append([]PeerMeta(nil), meta.Peers...)
	}
	return cp
}

// CloneRegionMetas returns a deep copy of the provided region map.
func CloneRegionMetas(src map[uint64]RegionMeta) map[uint64]RegionMeta {
	if len(src) == 0 {
		return nil
	}
	out := make(map[uint64]RegionMeta, len(src))
	for id, meta := range src {
		out[id] = CloneRegionMeta(meta)
	}
	return out
}

// CloneRegionMetaPtr returns a detached copy of one region metadata pointer.
func CloneRegionMetaPtr(meta *RegionMeta) *RegionMeta {
	if meta == nil {
		return nil
	}
	cp := CloneRegionMeta(*meta)
	return &cp
}

// RaftLogPointer tracks local WAL progress for one raft group.
//
// It is store-local recovery metadata used by raft replay, WAL GC, and
// diagnostics. It is not cluster authority.
type RaftLogPointer struct {
	GroupID         uint64 `json:"group_id"`
	Segment         uint32 `json:"segment"`
	Offset          uint64 `json:"offset"`
	AppliedIndex    uint64 `json:"applied_index"`
	AppliedTerm     uint64 `json:"applied_term"`
	Committed       uint64 `json:"committed"`
	SnapshotIndex   uint64 `json:"snapshot_index"`
	SnapshotTerm    uint64 `json:"snapshot_term"`
	TruncatedIndex  uint64 `json:"truncated_index"`
	TruncatedTerm   uint64 `json:"truncated_term"`
	SegmentIndex    uint64 `json:"segment_index"`
	TruncatedOffset uint64 `json:"truncated_offset"`
}

// CloneRaftPointers returns a detached copy of the provided raft pointer map.
func CloneRaftPointers(src map[uint64]RaftLogPointer) map[uint64]RaftLogPointer {
	if len(src) == 0 {
		return nil
	}
	out := make(map[uint64]RaftLogPointer, len(src))
	maps.Copy(out, src)
	return out
}
