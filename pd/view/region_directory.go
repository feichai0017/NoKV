package view

import (
	"bytes"
	"fmt"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"sort"
	"sync"
	"time"
)

// RegionInfo captures region metadata with heartbeat timestamp.
type RegionInfo struct {
	Meta          localmeta.RegionMeta `json:"meta"`
	LastHeartbeat time.Time            `json:"last_heartbeat"`
}

type regionIndexEntry struct {
	id    uint64
	start []byte
	end   []byte
}

// RegionDirectoryView is the disposable control-plane directory view used for
// route lookup and operator inspection.
type RegionDirectoryView struct {
	mu           sync.RWMutex
	regions      map[uint64]localmeta.RegionMeta
	regionLastHB map[uint64]time.Time
	regionIndex  []regionIndexEntry
}

func NewRegionDirectoryView() *RegionDirectoryView {
	return &RegionDirectoryView{
		regions:      make(map[uint64]localmeta.RegionMeta),
		regionLastHB: make(map[uint64]time.Time),
	}
}

func (v *RegionDirectoryView) Upsert(meta localmeta.RegionMeta) error {
	return v.UpsertAt(meta, time.Now())
}

func (v *RegionDirectoryView) UpsertAt(meta localmeta.RegionMeta, now time.Time) error {
	if v == nil {
		return nil
	}
	if meta.ID == 0 {
		return ErrInvalidRegionID
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	current, exists := v.regions[meta.ID]
	if exists && isEpochStale(meta.Epoch, current.Epoch) {
		return fmt.Errorf("%w: region=%d current={ver:%d conf:%d} incoming={ver:%d conf:%d}",
			ErrRegionHeartbeatStale,
			meta.ID,
			current.Epoch.Version, current.Epoch.ConfVersion,
			meta.Epoch.Version, meta.Epoch.ConfVersion,
		)
	}

	if overlapID, ok := v.findOverlapLocked(meta); ok {
		return fmt.Errorf("%w: region=%d overlaps region=%d", ErrRegionRangeOverlap, meta.ID, overlapID)
	}

	v.regions[meta.ID] = localmeta.CloneRegionMeta(meta)
	v.regionLastHB[meta.ID] = now
	v.rebuildIndexLocked()
	return nil
}

func (v *RegionDirectoryView) Remove(regionID uint64) bool {
	if v == nil || regionID == 0 {
		return false
	}
	v.mu.Lock()
	_, existed := v.regions[regionID]
	delete(v.regions, regionID)
	delete(v.regionLastHB, regionID)
	v.rebuildIndexLocked()
	v.mu.Unlock()
	return existed
}

func (v *RegionDirectoryView) Snapshot() []RegionInfo {
	if v == nil {
		return nil
	}
	v.mu.RLock()
	out := make([]RegionInfo, 0, len(v.regions))
	for id, meta := range v.regions {
		out = append(out, RegionInfo{Meta: localmeta.CloneRegionMeta(meta), LastHeartbeat: v.regionLastHB[id]})
	}
	v.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Meta.ID < out[j].Meta.ID })
	return out
}

func (v *RegionDirectoryView) Lookup(key []byte) (localmeta.RegionMeta, bool) {
	if v == nil {
		return localmeta.RegionMeta{}, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if len(v.regionIndex) == 0 {
		return localmeta.RegionMeta{}, false
	}
	idx := sort.Search(len(v.regionIndex), func(i int) bool {
		return bytes.Compare(v.regionIndex[i].start, key) > 0
	})
	if idx == 0 {
		return localmeta.RegionMeta{}, false
	}
	entry := v.regionIndex[idx-1]
	if bytes.Compare(key, entry.start) < 0 {
		return localmeta.RegionMeta{}, false
	}
	if len(entry.end) > 0 && bytes.Compare(key, entry.end) >= 0 {
		return localmeta.RegionMeta{}, false
	}
	meta, ok := v.regions[entry.id]
	if !ok {
		return localmeta.RegionMeta{}, false
	}
	return localmeta.CloneRegionMeta(meta), true
}

func (v *RegionDirectoryView) LastHeartbeat(regionID uint64) (time.Time, bool) {
	if v == nil || regionID == 0 {
		return time.Time{}, false
	}
	v.mu.RLock()
	ts, ok := v.regionLastHB[regionID]
	v.mu.RUnlock()
	return ts, ok
}

func (v *RegionDirectoryView) findOverlapLocked(meta localmeta.RegionMeta) (uint64, bool) {
	for id, existing := range v.regions {
		if id == meta.ID {
			continue
		}
		if rangesOverlap(meta, existing) {
			return id, true
		}
	}
	return 0, false
}

func (v *RegionDirectoryView) rebuildIndexLocked() {
	index := make([]regionIndexEntry, 0, len(v.regions))
	for id, meta := range v.regions {
		index = append(index, regionIndexEntry{id: id, start: append([]byte(nil), meta.StartKey...), end: append([]byte(nil), meta.EndKey...)})
	}
	sort.Slice(index, func(i, j int) bool {
		if cmp := bytes.Compare(index[i].start, index[j].start); cmp != 0 {
			return cmp < 0
		}
		return index[i].id < index[j].id
	})
	v.regionIndex = index
}

func isEpochStale(incoming, current localmeta.RegionEpoch) bool {
	if incoming.Version < current.Version {
		return true
	}
	if incoming.Version == current.Version && incoming.ConfVersion < current.ConfVersion {
		return true
	}
	return false
}

func rangesOverlap(a, b localmeta.RegionMeta) bool {
	if len(a.EndKey) > 0 && bytes.Compare(a.EndKey, b.StartKey) <= 0 {
		return false
	}
	if len(b.EndKey) > 0 && bytes.Compare(b.EndKey, a.StartKey) <= 0 {
		return false
	}
	return true
}
