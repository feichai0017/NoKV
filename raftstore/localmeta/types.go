package localmeta

import "maps"

// PeerMeta describes a peer replica for a region.
type PeerMeta struct {
	StoreID uint64 `json:"store_id"`
	PeerID  uint64 `json:"peer_id"`
}

// RegionState enumerates the local lifecycle state of one region replica.
type RegionState uint8

const (
	RegionStateNew RegionState = iota
	RegionStateRunning
	RegionStateRemoving
	RegionStateTombstone
)

// RegionEpoch tracks metadata versioning for one region.
type RegionEpoch struct {
	Version     uint64 `json:"version"`
	ConfVersion uint64 `json:"conf_version"`
}

// RegionMeta captures region key range and peer membership.
//
// This is shared by raftstore local recovery state and PD control-plane state.
// It is not part of the single-node storage manifest.
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
