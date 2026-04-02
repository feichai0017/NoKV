package store

import (
	"context"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"time"

	"github.com/feichai0017/NoKV/raftstore/descriptor"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// PeerBuilder constructs peer configuration for the provided region metadata.
// It allows the store to spawn new peers for splits without external callers
// wiring the configuration manually.
type PeerBuilder func(meta localmeta.RegionMeta) (*peer.Config, error)

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

func (t OperationType) String() string {
	switch t {
	case OperationLeaderTransfer:
		return "leader-transfer"
	default:
		return "none"
	}
}

// SchedulerStatus captures local/control-plane scheduler health. It is a
// diagnostic view only; stores must not treat it as routing authority.
type SchedulerMode string

const (
	SchedulerModeHealthy     SchedulerMode = "healthy"
	SchedulerModeDegraded    SchedulerMode = "degraded"
	SchedulerModeUnavailable SchedulerMode = "unavailable"
)

type SchedulerStatus struct {
	Mode              SchedulerMode `json:"mode"`
	Degraded          bool          `json:"degraded"`
	LastError         string        `json:"last_error,omitempty"`
	LastErrorAt       time.Time     `json:"last_error_at"`
	DroppedOperations uint64        `json:"dropped_operations"`
}

// SchedulerClient publishes store state to the control plane and returns any
// scheduling decisions that should be applied locally.
type SchedulerClient interface {
	PublishRegionDescriptor(context.Context, descriptor.Descriptor)
	RemoveRegion(context.Context, uint64)
	StoreHeartbeat(context.Context, StoreStats) []Operation
	Status() SchedulerStatus
	Close() error
}

// Config configures Store construction. Only the Router field is optional; the
// store fills in a default router when omitted.
type Config struct {
	Router             *Router
	PeerBuilder        PeerBuilder
	LocalMeta          *localmeta.Store
	WorkDir            string
	Scheduler          SchedulerClient
	HeartbeatInterval  time.Duration
	StoreID            uint64
	OperationQueueSize int
	OperationCooldown  time.Duration
	OperationInterval  time.Duration
	OperationBurst     int
	CommandApplier     func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	CommandTimeout     time.Duration
}
