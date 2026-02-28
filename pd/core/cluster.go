package core

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/manifest"
)

// StoreStats captures store-level heartbeat data tracked by PD.
type StoreStats struct {
	StoreID   uint64    `json:"store_id"`
	RegionNum uint64    `json:"region_num"`
	LeaderNum uint64    `json:"leader_num"`
	Capacity  uint64    `json:"capacity"`
	Available uint64    `json:"available"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RegionInfo captures region metadata with heartbeat timestamp.
type RegionInfo struct {
	Meta          manifest.RegionMeta `json:"meta"`
	LastHeartbeat time.Time           `json:"last_heartbeat"`
}

type regionIndexEntry struct {
	id    uint64
	start []byte
	end   []byte
}

// Cluster stores in-memory PD metadata and provides route lookups.
//
// NOTE: Cluster intentionally keeps only the in-memory metadata/state model.
// PD RPC wiring and persistence are handled by higher layers (pd/server and
// pd/storage).
type Cluster struct {
	mu sync.RWMutex

	stores map[uint64]StoreStats

	regions      map[uint64]manifest.RegionMeta
	regionLastHB map[uint64]time.Time
	regionIndex  []regionIndexEntry
}

// NewCluster creates an empty in-memory cluster metadata view.
func NewCluster() *Cluster {
	return &Cluster{
		stores:       make(map[uint64]StoreStats),
		regions:      make(map[uint64]manifest.RegionMeta),
		regionLastHB: make(map[uint64]time.Time),
	}
}

// UpsertStoreHeartbeat updates store metadata from a store heartbeat.
func (c *Cluster) UpsertStoreHeartbeat(stats StoreStats) error {
	if c == nil {
		return nil
	}
	if stats.StoreID == 0 {
		return ErrInvalidStoreID
	}
	stats.UpdatedAt = time.Now()
	c.mu.Lock()
	c.stores[stats.StoreID] = stats
	c.mu.Unlock()
	return nil
}

// RemoveStore removes a store from PD metadata.
func (c *Cluster) RemoveStore(storeID uint64) {
	if c == nil || storeID == 0 {
		return
	}
	c.mu.Lock()
	delete(c.stores, storeID)
	c.mu.Unlock()
}

// StoreSnapshot returns a stable copy of tracked store metadata.
func (c *Cluster) StoreSnapshot() []StoreStats {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	out := make([]StoreStats, 0, len(c.stores))
	for _, st := range c.stores {
		out = append(out, st)
	}
	c.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].StoreID < out[j].StoreID })
	return out
}

// UpsertRegionHeartbeat updates region metadata from a region heartbeat.
//
// It rejects stale epoch heartbeats and metadata that overlaps other regions.
func (c *Cluster) UpsertRegionHeartbeat(meta manifest.RegionMeta) error {
	if c == nil {
		return nil
	}
	if meta.ID == 0 {
		return ErrInvalidRegionID
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	current, exists := c.regions[meta.ID]
	if exists && isEpochStale(meta.Epoch, current.Epoch) {
		return fmt.Errorf("%w: region=%d current={ver:%d conf:%d} incoming={ver:%d conf:%d}",
			ErrRegionHeartbeatStale,
			meta.ID,
			current.Epoch.Version, current.Epoch.ConfVersion,
			meta.Epoch.Version, meta.Epoch.ConfVersion,
		)
	}

	if overlapID, ok := c.findOverlapLocked(meta); ok {
		return fmt.Errorf("%w: region=%d overlaps region=%d", ErrRegionRangeOverlap, meta.ID, overlapID)
	}

	c.regions[meta.ID] = manifest.CloneRegionMeta(meta)
	c.regionLastHB[meta.ID] = time.Now()
	c.rebuildRegionIndexLocked()
	return nil
}

// RemoveRegion removes a region from PD metadata and reports whether the region
// existed before removal.
func (c *Cluster) RemoveRegion(regionID uint64) bool {
	if c == nil || regionID == 0 {
		return false
	}
	c.mu.Lock()
	_, existed := c.regions[regionID]
	delete(c.regions, regionID)
	delete(c.regionLastHB, regionID)
	c.rebuildRegionIndexLocked()
	c.mu.Unlock()
	return existed
}

// RegionSnapshot returns a stable copy of tracked region metadata.
func (c *Cluster) RegionSnapshot() []RegionInfo {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	out := make([]RegionInfo, 0, len(c.regions))
	for id, meta := range c.regions {
		out = append(out, RegionInfo{
			Meta:          manifest.CloneRegionMeta(meta),
			LastHeartbeat: c.regionLastHB[id],
		})
	}
	c.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Meta.ID < out[j].Meta.ID })
	return out
}

// GetRegionByKey returns the region containing key ([start, end)).
func (c *Cluster) GetRegionByKey(key []byte) (manifest.RegionMeta, bool) {
	if c == nil {
		return manifest.RegionMeta{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.regionIndex) == 0 {
		return manifest.RegionMeta{}, false
	}

	idx := sort.Search(len(c.regionIndex), func(i int) bool {
		return bytes.Compare(c.regionIndex[i].start, key) > 0
	})
	if idx == 0 {
		return manifest.RegionMeta{}, false
	}
	entry := c.regionIndex[idx-1]
	if bytes.Compare(key, entry.start) < 0 {
		return manifest.RegionMeta{}, false
	}
	if len(entry.end) > 0 && bytes.Compare(key, entry.end) >= 0 {
		return manifest.RegionMeta{}, false
	}
	meta, ok := c.regions[entry.id]
	if !ok {
		return manifest.RegionMeta{}, false
	}
	return manifest.CloneRegionMeta(meta), true
}

// RegionLastHeartbeat returns the latest heartbeat timestamp for regionID.
func (c *Cluster) RegionLastHeartbeat(regionID uint64) (time.Time, bool) {
	if c == nil || regionID == 0 {
		return time.Time{}, false
	}
	c.mu.RLock()
	ts, ok := c.regionLastHB[regionID]
	c.mu.RUnlock()
	return ts, ok
}

func (c *Cluster) findOverlapLocked(meta manifest.RegionMeta) (uint64, bool) {
	for id, existing := range c.regions {
		if id == meta.ID {
			continue
		}
		if rangesOverlap(meta, existing) {
			return id, true
		}
	}
	return 0, false
}

func (c *Cluster) rebuildRegionIndexLocked() {
	index := make([]regionIndexEntry, 0, len(c.regions))
	for id, meta := range c.regions {
		index = append(index, regionIndexEntry{
			id:    id,
			start: append([]byte(nil), meta.StartKey...),
			end:   append([]byte(nil), meta.EndKey...),
		})
	}
	sort.Slice(index, func(i, j int) bool {
		if cmp := bytes.Compare(index[i].start, index[j].start); cmp != 0 {
			return cmp < 0
		}
		return index[i].id < index[j].id
	})
	c.regionIndex = index
}

func isEpochStale(incoming, current manifest.RegionEpoch) bool {
	if incoming.Version < current.Version {
		return true
	}
	if incoming.Version == current.Version && incoming.ConfVersion < current.ConfVersion {
		return true
	}
	return false
}

func rangesOverlap(a, b manifest.RegionMeta) bool {
	// [a.start, a.end) is fully before [b.start, b.end)
	if len(a.EndKey) > 0 && bytes.Compare(a.EndKey, b.StartKey) <= 0 {
		return false
	}
	// [b.start, b.end) is fully before [a.start, a.end)
	if len(b.EndKey) > 0 && bytes.Compare(b.EndKey, a.StartKey) <= 0 {
		return false
	}
	return true
}
