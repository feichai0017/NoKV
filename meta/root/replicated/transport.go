// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import raftpb "go.etcd.io/raft/v3/raftpb"

// MessageHandler consumes one incoming raft message for the replicated
// metadata root backend.
type MessageHandler func(raftpb.Message) error

// Transport carries raft messages between replicated metadata root nodes.
// The first implementation is gRPC-backed; higher layers should depend on this
// narrow surface instead of in-process routing details.
type Transport interface {
	Addr() string
	SetHandler(MessageHandler)
	SetPeer(id uint64, addr string)
	SetPeers(peers map[uint64]string)
	Send(msgs ...raftpb.Message) error
	Close() error
}
