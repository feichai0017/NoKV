package store

import (
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"time"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

// PeerBuilder constructs peer configuration for the provided region metadata.
// It allows the store to spawn new peers for splits without external callers
// wiring the configuration manually.
type PeerBuilder func(meta localmeta.RegionMeta) (*peer.Config, error)

// Config configures Store construction. Only the Router field is optional; the
// store fills in a default router when omitted.
type Config struct {
	Router             *Router
	PeerBuilder        PeerBuilder
	LocalMeta          *localmeta.Store
	WorkDir            string
	Scheduler          scheduler.Client
	HeartbeatInterval  time.Duration
	HeartbeatTimeout   time.Duration
	PublishTimeout     time.Duration
	StoreID            uint64
	ClientAddr         string
	RaftAddr           string
	OperationQueueSize int
	OperationCooldown  time.Duration
	OperationInterval  time.Duration
	OperationBurst     int
	CommandApplier     func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	CommandTimeout     time.Duration
}
