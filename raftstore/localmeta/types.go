package localmeta

import (
	"maps"
	"strconv"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

const (
	RegionStateNew       metaregion.ReplicaState = metaregion.ReplicaStateNew
	RegionStateRunning   metaregion.ReplicaState = metaregion.ReplicaStateRunning
	RegionStateRemoving  metaregion.ReplicaState = metaregion.ReplicaStateRemoving
	RegionStateTombstone metaregion.ReplicaState = metaregion.ReplicaStateTombstone
)

// RegionMeta captures the store-local runtime and recovery shape of one region.
//
// It belongs to raftstore local state, peer lifecycle, and restart recovery.
// It is intentionally smaller than the distributed topology descriptor and must
// not become control-plane authority.
type RegionMeta struct {
	ID       uint64                  `json:"id"`
	StartKey []byte                  `json:"start_key,omitempty"`
	EndKey   []byte                  `json:"end_key,omitempty"`
	Epoch    metaregion.Epoch        `json:"epoch"`
	Peers    []metaregion.Peer       `json:"peers,omitempty"`
	State    metaregion.ReplicaState `json:"state"`
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
		cp.Peers = append([]metaregion.Peer(nil), meta.Peers...)
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

// PendingRootEvent captures one store-local rooted event that has already been
// applied by raftstore but has not yet been acknowledged by Coordinator as
// published into meta/root.
//
// This is the execution-plane to control-plane durability bridge. If a store
// crashes after local raft apply but before Coordinator publish succeeds, the
// recovered store replays these entries and retries publish instead of losing
// terminal topology truth. Tenure controls which Coordinator may
// accept singleton duties, but the store still persists these events locally
// until publish acknowledgement removes them.
type PendingRootEvent struct {
	Sequence uint64          `json:"sequence"`
	Event    rootevent.Event `json:"event"`
}

// ClonePendingRootEvent returns a deep copy of one pending rooted event.
func ClonePendingRootEvent(event PendingRootEvent) PendingRootEvent {
	cp := event
	cp.Event = rootevent.CloneEvent(event.Event)
	return cp
}

// ClonePendingRootEvents returns a detached copy of the provided pending rooted
// event map.
func ClonePendingRootEvents(src map[uint64]PendingRootEvent) map[uint64]PendingRootEvent {
	if len(src) == 0 {
		return nil
	}
	out := make(map[uint64]PendingRootEvent, len(src))
	for seq, event := range src {
		out[seq] = ClonePendingRootEvent(event)
	}
	return out
}

type PendingSchedulerOperationKind uint8

const (
	PendingSchedulerOperationUnknown PendingSchedulerOperationKind = iota
	PendingSchedulerOperationLeaderTransfer
)

// PendingSchedulerOperation captures one store-local scheduler decision that
// must survive shutdown and restart until the local executor applies it.
//
// This is not cluster authority. It is a durability bridge for control-plane
// decisions that have been accepted by the store runtime but not yet consumed
// by the local execution loop.
type PendingSchedulerOperation struct {
	Kind         PendingSchedulerOperationKind `json:"kind"`
	RegionID     uint64                        `json:"region_id"`
	SourcePeerID uint64                        `json:"source_peer_id,omitempty"`
	TargetPeerID uint64                        `json:"target_peer_id,omitempty"`
	Attempts     uint32                        `json:"attempts,omitempty"`
}

func ClonePendingSchedulerOperation(op PendingSchedulerOperation) PendingSchedulerOperation {
	return op
}

func ClonePendingSchedulerOperations(src map[string]PendingSchedulerOperation) map[string]PendingSchedulerOperation {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]PendingSchedulerOperation, len(src))
	for key, op := range src {
		out[key] = ClonePendingSchedulerOperation(op)
	}
	return out
}

func PendingSchedulerOperationKey(op PendingSchedulerOperation) string {
	return pendingSchedulerOperationKey(op.Kind, op.RegionID)
}

func pendingSchedulerOperationKey(kind PendingSchedulerOperationKind, regionID uint64) string {
	return kind.String() + ":" + strconv.FormatUint(regionID, 10)
}

func (k PendingSchedulerOperationKind) String() string {
	switch k {
	case PendingSchedulerOperationLeaderTransfer:
		return "leader-transfer"
	default:
		return "unknown"
	}
}

// BlockedRootEvent captures one locally applied rooted event that must not be
// retried automatically because the coordinator rejected it permanently.
//
// The event remains durable so restart recovery and operator diagnostics can
// detect the local-vs-rooted divergence explicitly instead of retrying
// forever.
type BlockedRootEvent struct {
	Sequence     uint64          `json:"sequence"`
	Event        rootevent.Event `json:"event"`
	TransitionID string          `json:"transition_id,omitempty"`
	LastError    string          `json:"last_error,omitempty"`
}

func CloneBlockedRootEvent(event BlockedRootEvent) BlockedRootEvent {
	cp := event
	cp.Event = rootevent.CloneEvent(event.Event)
	return cp
}

func CloneBlockedRootEvents(src map[uint64]BlockedRootEvent) map[uint64]BlockedRootEvent {
	if len(src) == 0 {
		return nil
	}
	out := make(map[uint64]BlockedRootEvent, len(src))
	for seq, event := range src {
		out[seq] = CloneBlockedRootEvent(event)
	}
	return out
}
