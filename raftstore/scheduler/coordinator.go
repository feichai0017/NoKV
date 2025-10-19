package scheduler

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/manifest"
)

// RegionSink receives region heartbeat events from raftstore when metadata
// changes occur or regions are removed.
type RegionSink interface {
	SubmitRegionHeartbeat(manifest.RegionMeta)
	RemoveRegion(uint64)
	SubmitStoreHeartbeat(StoreStats)
}

// SnapshotProvider exposes aggregated cluster state to schedulers or tooling.
type SnapshotProvider interface {
	RegionSnapshot() []manifest.RegionMeta
	StoreSnapshot() []StoreStats
}

// Coordinator captures region heartbeats and provides a snapshot for schedulers
// to reason about cluster topology.
type Coordinator struct {
	mu          sync.RWMutex
	regions     map[uint64]manifest.RegionMeta
	lastUpdated map[uint64]time.Time
	stores      map[uint64]StoreStats
}

// NewCoordinator constructs a Coordinator with in-memory storage.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		regions:     make(map[uint64]manifest.RegionMeta),
		lastUpdated: make(map[uint64]time.Time),
		stores:      make(map[uint64]StoreStats),
	}
}

// SubmitRegionHeartbeat records the provided region metadata.
func (c *Coordinator) SubmitRegionHeartbeat(meta manifest.RegionMeta) {
	if c == nil || meta.ID == 0 {
		return
	}
	c.mu.Lock()
	c.regions[meta.ID] = manifest.CloneRegionMeta(meta)
	c.lastUpdated[meta.ID] = time.Now()
	c.mu.Unlock()
}

// RemoveRegion deletes the region from the coordinator's view.
func (c *Coordinator) RemoveRegion(id uint64) {
	if c == nil || id == 0 {
		return
	}
	c.mu.Lock()
	delete(c.regions, id)
	delete(c.lastUpdated, id)
	c.mu.Unlock()
}

// RegionSnapshot returns a slice copy of the known regions. Callers receive
// cloned metadata and may mutate the result freely.
func (c *Coordinator) RegionSnapshot() []manifest.RegionMeta {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	out := make([]manifest.RegionMeta, 0, len(c.regions))
	for _, meta := range c.regions {
		out = append(out, manifest.CloneRegionMeta(meta))
	}
	c.mu.RUnlock()
	return out
}

// LastUpdate returns the timestamp a heartbeat was last recorded for the region
// and a boolean indicating whether the region exists.
func (c *Coordinator) LastUpdate(id uint64) (time.Time, bool) {
	if c == nil || id == 0 {
		return time.Time{}, false
	}
	c.mu.RLock()
	ts, ok := c.lastUpdated[id]
	c.mu.RUnlock()
	return ts, ok
}

// StoreStats captures minimal store-level heartbeat information.
type StoreStats struct {
	StoreID   uint64    `json:"store_id"`
	RegionNum uint64    `json:"region_num"`
	LeaderNum uint64    `json:"leader_num"`
	Capacity  uint64    `json:"capacity"`
	Available uint64    `json:"available"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SubmitStoreHeartbeat records store statistics.
func (c *Coordinator) SubmitStoreHeartbeat(stats StoreStats) {
	if c == nil || stats.StoreID == 0 {
		return
	}
	stats.UpdatedAt = time.Now()
	c.mu.Lock()
	c.stores[stats.StoreID] = stats
	c.mu.Unlock()
}

// StoreSnapshot returns the currently tracked store heartbeats.
func (c *Coordinator) StoreSnapshot() []StoreStats {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	out := make([]StoreStats, 0, len(c.stores))
	for _, stats := range c.stores {
		out = append(out, stats)
	}
	c.mu.RUnlock()
	return out
}

// RemoveStore removes store-level stats from the coordinator.
func (c *Coordinator) RemoveStore(storeID uint64) {
	if c == nil || storeID == 0 {
		return
	}
	c.mu.Lock()
	delete(c.stores, storeID)
	c.mu.Unlock()
}
