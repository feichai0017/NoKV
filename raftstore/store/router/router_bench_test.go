// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
)

// BenchmarkPeerLookupMiss measures the lookup hot path on a router with no
// matching peer. Lookup runs per-message so the cost shows up under high
// raft message rates even when most lookups miss (e.g. snapshot bootstrap).
func BenchmarkPeerLookupMiss(b *testing.B) {
	r := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = r.Peer(uint64(i + 1))
	}
}

// BenchmarkSendRaftMissingPeer measures the message-dispatch path when the
// recipient is unknown — common during peer lifecycle transitions.
func BenchmarkSendRaftMissingPeer(b *testing.B) {
	r := New()
	msg := myraft.Message{To: 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.SendRaft(uint64(i+1), msg)
	}
}

// BenchmarkListEmpty measures the snapshot allocation for an empty peer set.
// List runs once per BroadcastTick / BroadcastFlush.
func BenchmarkListEmpty(b *testing.B) {
	r := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.List()
	}
}
