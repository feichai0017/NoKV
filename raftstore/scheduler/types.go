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
	// DrainOperations returns and drains pending scheduling operations that
	// should be executed by the store runtime.
	DrainOperations() []Operation
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
