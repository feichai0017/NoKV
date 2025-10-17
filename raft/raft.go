package raft

import (
	etcdraft "go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type (
	// Aliases to etcd/raft exposed types so callers don't import the dependency directly.
	StateType        = etcdraft.StateType
	Config           = etcdraft.Config
	RawNode          = etcdraft.RawNode
	Ready            = etcdraft.Ready
	SoftState        = etcdraft.SoftState
	Status           = etcdraft.Status
	Peer             = etcdraft.Peer
	MemoryStorage    = etcdraft.MemoryStorage
	Storage          = etcdraft.Storage
	HardState        = pb.HardState
	Snapshot         = pb.Snapshot
	SnapshotMetadata = pb.SnapshotMetadata
	Message          = pb.Message
	MessageType      = pb.MessageType
	Entry            = pb.Entry
	EntryType        = pb.EntryType
	ConfState        = pb.ConfState
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
