// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type (
	// Aliases to etcd/raft exposed types so callers don't import the dependency directly.
	StateType        = etcdraft.StateType
	Config           = etcdraft.Config
	ReadOnlyOption   = etcdraft.ReadOnlyOption
	RawNode          = etcdraft.RawNode
	Ready            = etcdraft.Ready
	SoftState        = etcdraft.SoftState
	BasicStatus      = etcdraft.BasicStatus
	Status           = etcdraft.Status
	Peer             = etcdraft.Peer
	ReadState        = etcdraft.ReadState
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
	Logger           = etcdraft.Logger
	DefaultLogger    = etcdraft.DefaultLogger
)

const (
	StateFollower     = etcdraft.StateFollower
	StateCandidate    = etcdraft.StateCandidate
	StateLeader       = etcdraft.StateLeader
	StatePreCandidate = etcdraft.StatePreCandidate
)

const (
	ReadOnlySafe       = etcdraft.ReadOnlySafe
	ReadOnlyLeaseBased = etcdraft.ReadOnlyLeaseBased
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
	IsEmptyHardState = etcdraft.IsEmptyHardState
	IsEmptySnap      = etcdraft.IsEmptySnap
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
