package store

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

type regionStatsRuntime struct {
	mu       sync.Mutex
	last     time.Time
	counters map[uint64]regionTrafficCounter
}

type regionTrafficCounter struct {
	readOps    uint64
	writeOps   uint64
	writeBytes uint64
	atomicOps  uint64
}

func newRegionStatsRuntime() *regionStatsRuntime {
	return &regionStatsRuntime{
		last:     time.Now(),
		counters: make(map[uint64]regionTrafficCounter),
	}
}

func (r *regionStatsRuntime) recordRead(regionID uint64, ops uint64) {
	if r == nil || regionID == 0 || ops == 0 {
		return
	}
	r.mu.Lock()
	counter := r.counters[regionID]
	counter.readOps += ops
	r.counters[regionID] = counter
	r.mu.Unlock()
}

func (r *regionStatsRuntime) recordApply(evt ApplyEvent) {
	if r == nil || evt.RegionID == 0 {
		return
	}
	var keyBytes uint64
	for _, key := range evt.Keys {
		keyBytes += uint64(len(key))
	}
	r.mu.Lock()
	counter := r.counters[evt.RegionID]
	counter.writeOps++
	counter.writeBytes += keyBytes
	if evt.AtomicMutate {
		counter.atomicOps++
	}
	r.counters[evt.RegionID] = counter
	r.mu.Unlock()
}

func (r *regionStatsRuntime) snapshot(metas []localmeta.RegionMeta, leaderStoreID uint64, leaderRegions map[uint64]struct{}, pending map[uint64]bool) []storecontrol.RegionStats {
	if r == nil {
		return nil
	}
	now := time.Now()
	r.mu.Lock()
	elapsed := now.Sub(r.last)
	if elapsed <= 0 {
		elapsed = time.Second
	}
	counters := r.counters
	r.counters = make(map[uint64]regionTrafficCounter)
	r.last = now
	r.mu.Unlock()

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
