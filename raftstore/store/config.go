package store

import (
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// PeerBuilder constructs peer configuration for the provided region metadata.
// It allows the store to spawn new peers for splits without external callers
// wiring the configuration manually.
type PeerBuilder func(meta manifest.RegionMeta) (*peer.Config, error)

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

// SchedulerClient publishes store state to the control plane and returns any
// scheduling decisions that should be applied locally.
type SchedulerClient interface {
	PublishRegion(manifest.RegionMeta)
	RemoveRegion(uint64)
	StoreHeartbeat(StoreStats) []Operation
	Close() error
}

// Config configures Store construction. Only the Router field is optional; the
// store fills in a default router when omitted.
type Config struct {
	Router             *Router
	PeerBuilder        PeerBuilder
	Manifest           *manifest.Manager
	Scheduler          SchedulerClient
	HeartbeatInterval  time.Duration
	StoreID            uint64
	OperationQueueSize int
	OperationCooldown  time.Duration
	OperationInterval  time.Duration
	OperationBurst     int
	CommandApplier     func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)
	CommandTimeout     time.Duration
}
