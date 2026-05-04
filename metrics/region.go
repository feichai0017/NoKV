package metrics

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"sync"
)

// RegionMetrics records region counts grouped by lifecycle state. It is safe for concurrent use.
type RegionMetrics struct {
	mu        sync.Mutex
	total     uint64
	stateByID map[uint64]metaregion.ReplicaState
	counts    map[metaregion.ReplicaState]uint64
}

// RegionMetricsSnapshot captures point-in-time counts per region state.
type RegionMetricsSnapshot struct {
	Total     uint64 `json:"total"`
	New       uint64 `json:"new"`
	Running   uint64 `json:"running"`
	Removing  uint64 `json:"removing"`
	Tombstone uint64 `json:"tombstone"`
	Other     uint64 `json:"other"`
}

// NewRegionMetrics creates an empty recorder.
func NewRegionMetrics() *RegionMetrics {
	return &RegionMetrics{
		stateByID: make(map[uint64]metaregion.ReplicaState),
		counts:    make(map[metaregion.ReplicaState]uint64),
	}
}

// Snapshot returns the current counts.
func (rm *RegionMetrics) Snapshot() RegionMetricsSnapshot {
	if rm == nil {
		return RegionMetricsSnapshot{}
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	snap := RegionMetricsSnapshot{Total: rm.total}
	for state, count := range rm.counts {
		switch state {
		case metaregion.ReplicaStateNew:
			snap.New = count
		case metaregion.ReplicaStateRunning:
			snap.Running = count
		case metaregion.ReplicaStateRemoving:
			snap.Removing = count
		case metaregion.ReplicaStateTombstone:
			snap.Tombstone = count
		default:
			snap.Other += count
		}
	}
	return snap
}

// RecordState updates the metrics snapshot for one region lifecycle state.
// Callers pass plain identity and state so metrics does not depend on
// raftstore-local catalog structs.
func (rm *RegionMetrics) RecordState(regionID uint64, state metaregion.ReplicaState) {
	if rm == nil || regionID == 0 {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	prev, exists := rm.stateByID[regionID]
	if exists {
		if prev == state {
			return
		}
		rm.decrement(prev)
	} else {
		rm.total++
	}
	rm.increment(state)
	rm.stateByID[regionID] = state
}

// RecordRemove updates the metrics snapshot for a region removal.
func (rm *RegionMetrics) RecordRemove(regionID uint64) {
	if rm == nil || regionID == 0 {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if state, ok := rm.stateByID[regionID]; ok {
		rm.decrement(state)
		delete(rm.stateByID, regionID)
		if rm.total > 0 {
			rm.total--
		}
	}
}

func (rm *RegionMetrics) increment(state metaregion.ReplicaState) {
	rm.counts[state] = rm.counts[state] + 1
}

func (rm *RegionMetrics) decrement(state metaregion.ReplicaState) {
	if curr, ok := rm.counts[state]; ok {
		if curr <= 1 {
			delete(rm.counts, state)
		} else {
			rm.counts[state] = curr - 1
		}
	}
}
