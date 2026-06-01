// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package storecontrol defines the control-plane channel shared by store
// runtimes and coordinator-backed control clients.
package storecontrol

import (
	"context"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

// StoreStats captures minimal store-level heartbeat information.
type StoreStats struct {
	StoreID    uint64 `json:"store_id"`
	ClientAddr string `json:"client_addr,omitempty"`
	RaftAddr   string `json:"raft_addr,omitempty"`
	RegionNum  uint64 `json:"region_num"`
	LeaderNum  uint64 `json:"leader_num"`
	// LeaderRegionIDs enumerates the regions for which this store is the
	// local raft leader at snapshot time. The coordinator uses this to
	// populate its region directory view with per-region leadership.
	LeaderRegionIDs   []uint64      `json:"leader_region_ids,omitempty"`
	Capacity          uint64        `json:"capacity"`
	Available         uint64        `json:"available"`
	DroppedOperations uint64        `json:"dropped_operations"`
	UpdatedAt         time.Time     `json:"updated_at"`
	RegionStats       []RegionStats `json:"region_stats,omitempty"`
}

// RegionStats carries low-cardinality per-region load over the store-control
// heartbeat. It is scheduling input only; region descriptors remain rooted
// truth and must not be inferred from this telemetry.
type RegionStats struct {
	RegionID            uint64 `json:"region_id"`
	ReadQPS             uint64 `json:"read_qps"`
	WriteQPS            uint64 `json:"write_qps"`
	WriteBytesPerSecond uint64 `json:"write_bytes_per_sec"`
	ApproxRegionBytes   uint64 `json:"approx_region_bytes"`
	AtomicMutateQPS     uint64 `json:"atomic_mutate_qps"`
	LeaderStoreID       uint64 `json:"leader_store_id,omitempty"`
	PendingAdmin        bool   `json:"pending_admin,omitempty"`
}

// Operation represents a control-plane decision to be executed by store runtime.
type Operation struct {
	Type           OperationType
	Region         uint64
	Source         uint64
	Target         uint64
	RetentionFloor uint64
}

// OperationType identifies one store-control operation kind.
type OperationType uint8

const (
	OperationNone OperationType = iota
	OperationLeaderTransfer
	OperationPruneMetadataVersions
)

func (t OperationType) String() string {
	switch t {
	case OperationLeaderTransfer:
		return "leader-transfer"
	case OperationPruneMetadataVersions:
		return "prune-metadata-versions"
	default:
		return "none"
	}
}

// Mode classifies store-control health. It is diagnostic only; stores must not
// treat it as routing authority.
type Mode string

const (
	ModeHealthy     Mode = "healthy"
	ModeDegraded    Mode = "degraded"
	ModeUnavailable Mode = "unavailable"
)

// Status captures local/control-plane channel health.
type Status struct {
	Mode              Mode      `json:"mode"`
	Degraded          bool      `json:"degraded"`
	LastError         string    `json:"last_error,omitempty"`
	LastErrorAt       time.Time `json:"last_error_at"`
	DroppedOperations uint64    `json:"dropped_operations"`
}

// Client publishes store state and rooted events to the control plane, then
// returns any decisions that should be applied locally.
type Client interface {
	// ReportRegionHeartbeat reports one runtime region-liveness heartbeat.
	ReportRegionHeartbeat(context.Context, uint64)
	PublishRootEvent(context.Context, rootevent.Event) error
	StoreHeartbeat(context.Context, StoreStats) []Operation
	Status() Status
	Close() error
}
