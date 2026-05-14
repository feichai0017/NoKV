// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// Stats accumulates per-region read/write counters between scheduler
// heartbeat snapshots.
type Stats struct {
	mu       sync.Mutex
	last     time.Time
	counters map[uint64]trafficCounter
}

type trafficCounter struct {
	readOps    uint64
	writeOps   uint64
	writeBytes uint64
	atomicOps  uint64
}

// NewStats constructs an empty Stats accumulator.
func NewStats() *Stats {
	return &Stats{
		last:     time.Now(),
		counters: make(map[uint64]trafficCounter),
	}
}

// RecordRead adds ops read operations against regionID.
func (s *Stats) RecordRead(regionID uint64, ops uint64) {
	if s == nil || regionID == 0 || ops == 0 {
		return
	}
	s.mu.Lock()
	counter := s.counters[regionID]
	counter.readOps += ops
	s.counters[regionID] = counter
	s.mu.Unlock()
}

// RecordApply adds one write operation against regionID. keyBytes is the
// summed length of all keys in the apply batch; atomic flags atomic-mutate
// commands so the scheduler can size queue policies.
func (s *Stats) RecordApply(regionID uint64, keyBytes uint64, atomic bool) {
	if s == nil || regionID == 0 {
		return
	}
	s.mu.Lock()
	counter := s.counters[regionID]
	counter.writeOps++
	counter.writeBytes += keyBytes
	if atomic {
		counter.atomicOps++
	}
	s.counters[regionID] = counter
	s.mu.Unlock()
}

// Snapshot drains the accumulator and returns RegionStats for the supplied
// metas. leaderRegions marks regions whose local peer is currently the
// raft leader; pending marks regions with in-flight admin operations.
func (s *Stats) Snapshot(metas []localmeta.RegionMeta, leaderStoreID uint64, leaderRegions map[uint64]struct{}, pending map[uint64]bool) []storecontrol.RegionStats {
	if s == nil {
		return nil
	}
	now := time.Now()
	s.mu.Lock()
	elapsed := now.Sub(s.last)
	if elapsed <= 0 {
		elapsed = time.Second
	}
	counters := s.counters
	s.counters = make(map[uint64]trafficCounter)
	s.last = now
	s.mu.Unlock()

	seconds := uint64(elapsed / time.Second)
	if seconds == 0 {
		seconds = 1
	}
	out := make([]storecontrol.RegionStats, 0, len(metas))
	for _, meta := range metas {
		if meta.ID == 0 {
			continue
		}
		counter := counters[meta.ID]
		stat := storecontrol.RegionStats{
			RegionID:            meta.ID,
			ReadQPS:             counter.readOps / seconds,
			WriteQPS:            counter.writeOps / seconds,
			WriteBytesPerSecond: counter.writeBytes / seconds,
			AtomicMutateQPS:     counter.atomicOps / seconds,
			PendingAdmin:        pending[meta.ID],
		}
		if _, ok := leaderRegions[meta.ID]; ok {
			stat.LeaderStoreID = leaderStoreID
		}
		out = append(out, stat)
	}
	return out
}
