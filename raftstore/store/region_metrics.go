package store

import (
	"sync"

	"github.com/feichai0017/NoKV/manifest"
)

// RegionMetrics records region counts grouped by lifecycle state. It is safe
// for concurrent use.
type RegionMetrics struct {
	mu        sync.Mutex
	total     uint64
	stateByID map[uint64]manifest.RegionState
	counts    map[manifest.RegionState]uint64
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
		stateByID: make(map[uint64]manifest.RegionState),
		counts:    make(map[manifest.RegionState]uint64),
	}
}

// Hooks returns a RegionHooks structure wired to update the recorder.
func (rm *RegionMetrics) Hooks() RegionHooks {
	if rm == nil {
		return RegionHooks{}
	}
	return RegionHooks{
		OnRegionUpdate: rm.onRegionUpdate,
		OnRegionRemove: rm.onRegionRemove,
	}
}

// Snapshot returns the current counts.
func (rm *RegionMetrics) Snapshot() RegionMetricsSnapshot {
	if rm == nil {
		return RegionMetricsSnapshot{}
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	snap := RegionMetricsSnapshot{
		Total: rm.total,
	}
	for state, count := range rm.counts {
		switch state {
		case manifest.RegionStateNew:
			snap.New = count
		case manifest.RegionStateRunning:
			snap.Running = count
		case manifest.RegionStateRemoving:
			snap.Removing = count
		case manifest.RegionStateTombstone:
			snap.Tombstone = count
		default:
			snap.Other += count
		}
	}
	return snap
}

func (rm *RegionMetrics) onRegionUpdate(meta manifest.RegionMeta) {
	if rm == nil || meta.ID == 0 {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	prev, exists := rm.stateByID[meta.ID]
	if exists {
		if prev == meta.State {
			return
		}
		rm.decrement(prev)
	} else {
		rm.total++
	}
	rm.increment(meta.State)
	rm.stateByID[meta.ID] = meta.State
}

func (rm *RegionMetrics) onRegionRemove(regionID uint64) {
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

func (rm *RegionMetrics) increment(state manifest.RegionState) {
	rm.counts[state] = rm.counts[state] + 1
}

func (rm *RegionMetrics) decrement(state manifest.RegionState) {
	if curr, ok := rm.counts[state]; ok {
		if curr <= 1 {
			delete(rm.counts, state)
		} else {
			rm.counts[state] = curr - 1
		}
	}
}
