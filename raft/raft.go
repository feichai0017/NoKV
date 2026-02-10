package raft

import (
	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type (
	// Aliases to etcd/raft exposed types so callers don't import the dependency directly.
	StateType = etcdraft.StateType
	// Config defines an exported API type.
	Config = etcdraft.Config
	// RawNode defines an exported API type.
	RawNode = etcdraft.RawNode
	// Ready defines an exported API type.
	Ready = etcdraft.Ready
	// SoftState defines an exported API type.
	SoftState = etcdraft.SoftState
	// Status defines an exported API type.
	Status = etcdraft.Status
	// Peer defines an exported API type.
	Peer = etcdraft.Peer
	// ReadState defines an exported API type.
	ReadState = etcdraft.ReadState
	// MemoryStorage defines an exported API type.
	MemoryStorage = etcdraft.MemoryStorage
	// Storage defines an exported API type.
	Storage = etcdraft.Storage
	// HardState defines an exported API type.
	HardState = pb.HardState
	// Snapshot defines an exported API type.
	Snapshot = pb.Snapshot
	// SnapshotMetadata defines an exported API type.
	SnapshotMetadata = pb.SnapshotMetadata
	// Message defines an exported API type.
	Message = pb.Message
	// MessageType defines an exported API type.
	MessageType = pb.MessageType
	// Entry defines an exported API type.
	Entry = pb.Entry
	// EntryType defines an exported API type.
	EntryType = pb.EntryType
	// ConfState defines an exported API type.
	ConfState = pb.ConfState
	// Logger defines an exported API type.
	Logger = etcdraft.Logger
	// DefaultLogger defines an exported API type.
	DefaultLogger = etcdraft.DefaultLogger
)

const (
	StateFollower     = etcdraft.StateFollower
	StateCandidate    = etcdraft.StateCandidate
	StateLeader       = etcdraft.StateLeader
	StatePreCandidate = etcdraft.StatePreCandidate
)

const (
	MsgHup                 = pb.MsgHup
	MsgBeat                = pb.MsgBeat
	MsgPropose             = pb.MsgProp
	MsgAppend              = pb.MsgApp
	MsgAppendResponse      = pb.MsgAppResp
	MsgRequestVote         = pb.MsgVote
	MsgRequestVoteResponse = pb.MsgVoteResp
	MsgSnapshot            = pb.MsgSnap
	MsgSnapshotStatus      = pb.MsgSnapStatus
	MsgHeartbeat           = pb.MsgHeartbeat
	MsgHeartbeatResponse   = pb.MsgHeartbeatResp
	MsgTransferLeader      = pb.MsgTransferLeader
	MsgTimeoutNow          = pb.MsgTimeoutNow
)

const (
	EntryNormal       = pb.EntryNormal
	EntryConfChange   = pb.EntryConfChange
	EntryConfChangeV2 = pb.EntryConfChangeV2
)

var (
	ErrCompacted                      = etcdraft.ErrCompacted
	ErrSnapOutOfDate                  = etcdraft.ErrSnapOutOfDate
	ErrUnavailable                    = etcdraft.ErrUnavailable
	ErrSnapshotTemporarilyUnavailable = etcdraft.ErrSnapshotTemporarilyUnavailable
	ErrStop                           = etcdraft.ErrStopped
	IsEmptyHardState                  = etcdraft.IsEmptyHardState
	IsEmptySnap                       = etcdraft.IsEmptySnap
)

// NewMemoryStorage returns an in-memory Storage implementation.
func NewMemoryStorage() *MemoryStorage {
	return etcdraft.NewMemoryStorage()
}

// NewRawNode creates a new RawNode with the provided configuration.
func NewRawNode(cfg *Config) (*RawNode, error) {
	return etcdraft.NewRawNode(cfg)
}

// RestartRawNode restarts the RawNode from a new state.

// SetLogger installs a custom logger for raft. The logger is shared by all nodes
// in the process, mirroring etcd/raft's global logging behaviour.
func SetLogger(l Logger) {
	etcdraft.SetLogger(l)
}

// ResetDefaultLogger restores the default etcd/raft logger.
func ResetDefaultLogger() {
	etcdraft.ResetDefaultLogger()
}
