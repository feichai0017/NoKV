package scheduler

import (
	"time"

	"github.com/feichai0017/NoKV/manifest"
)

// RegionSink is the store-side publish interface for scheduling metadata.
//
// In cluster mode, implementations should forward to PD (the control-plane
// source of truth).
type RegionSink interface {
	SubmitRegionHeartbeat(manifest.RegionMeta)
	RemoveRegion(uint64)
	SubmitStoreHeartbeat(StoreStats)
}

// SnapshotProvider exposes a read-only view of aggregated scheduling state.
// This is used by diagnostics and tests only.
type SnapshotProvider interface {
	RegionSnapshot() []RegionInfo
	StoreSnapshot() []StoreStats
}

// Planner consumes a scheduler Snapshot and produces scheduling operations.
//
// In NoKV cluster mode, planner logic is expected to come from PD adapter.
type Planner interface {
	Plan(snapshot Snapshot) []Operation
}

// Snapshot aggregates scheduler-visible state for planning.
type Snapshot struct {
	Regions []RegionDescriptor
	Stores  []StoreStats
}

// StoreStats captures minimal store-level heartbeat information.
type StoreStats struct {
	StoreID   uint64    `json:"store_id"`
	RegionNum uint64    `json:"region_num"`
	LeaderNum uint64    `json:"leader_num"`
	Capacity  uint64    `json:"capacity"`
	Available uint64    `json:"available"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RegionInfo captures region metadata alongside heartbeat timestamp.
type RegionInfo struct {
	Meta          manifest.RegionMeta `json:"meta"`
	LastHeartbeat time.Time           `json:"last_heartbeat"`
}

// RegionDescriptor is a lightweight view of region metadata for planning.
type RegionDescriptor struct {
	ID            uint64
	StartKey      []byte
	EndKey        []byte
	Peers         []PeerDescriptor
	Epoch         manifest.RegionEpoch
	LastHeartbeat time.Time
	Lag           time.Duration
}

// PeerDescriptor describes a raft peer in a region.
type PeerDescriptor struct {
	StoreID uint64
	PeerID  uint64
	Leader  bool
}

// Operation represents a scheduling decision to be executed by store runtime.
type Operation struct {
	Type   OperationType
	Region uint64
	Source uint64
	Target uint64
}

// OperationType identifies the scheduler operation kind.
type OperationType uint8

const (
	OperationNone OperationType = iota
	OperationLeaderTransfer
)
