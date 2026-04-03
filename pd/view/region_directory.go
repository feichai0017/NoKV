package view

import (
	"bytes"
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"sort"
	"sync"
	"time"
)

// RegionInfo captures region metadata with heartbeat timestamp.
type RegionInfo struct {
	Descriptor    descriptor.Descriptor `json:"descriptor"`
	LastHeartbeat time.Time             `json:"last_heartbeat"`
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
	regions      map[uint64]descriptor.Descriptor
	regionLastHB map[uint64]time.Time
	regionIndex  []regionIndexEntry
}

func NewRegionDirectoryView() *RegionDirectoryView {
	return &RegionDirectoryView{
		regions:      make(map[uint64]descriptor.Descriptor),
		regionLastHB: make(map[uint64]time.Time),
	}
}

func (v *RegionDirectoryView) Upsert(desc descriptor.Descriptor) error {
	return v.UpsertAt(desc, time.Now())
}

func (v *RegionDirectoryView) UpsertAt(desc descriptor.Descriptor, now time.Time) error {
	if v == nil {
		return nil
	}
	if desc.RegionID == 0 {
		return ErrInvalidRegionID
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	current, exists := v.regions[desc.RegionID]
	if exists && isEpochStale(desc.Epoch, current.Epoch) {
		return fmt.Errorf("%w: region=%d current={ver:%d conf:%d} incoming={ver:%d conf:%d}",
			ErrRegionHeartbeatStale,
			desc.RegionID,
			current.Epoch.Version, current.Epoch.ConfVersion,
			desc.Epoch.Version, desc.Epoch.ConfVersion,
		)
	}

	if overlapID, ok := v.findOverlapLocked(desc); ok {
		return fmt.Errorf("%w: region=%d overlaps region=%d", ErrRegionRangeOverlap, desc.RegionID, overlapID)
	}

	v.regions[desc.RegionID] = desc.Clone()
	v.regionLastHB[desc.RegionID] = now
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
	for id, desc := range v.regions {
		out = append(out, RegionInfo{Descriptor: desc.Clone(), LastHeartbeat: v.regionLastHB[id]})
	}
	v.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Descriptor.RegionID < out[j].Descriptor.RegionID })
	return out
}

func (v *RegionDirectoryView) Replace(descriptors map[uint64]descriptor.Descriptor) {
	if v == nil {
		return
	}
	regions := make(map[uint64]descriptor.Descriptor, len(descriptors))
	heartbeats := make(map[uint64]time.Time, len(descriptors))
	now := time.Now()
	for id, desc := range descriptors {
		if id == 0 || desc.RegionID == 0 {
			continue
		}
		regions[id] = desc.Clone()
		heartbeats[id] = now
	}
	v.mu.Lock()
	v.regions = regions
	v.regionLastHB = heartbeats
	v.rebuildIndexLocked()
	v.mu.Unlock()
}

func (v *RegionDirectoryView) LookupDescriptor(key []byte) (descriptor.Descriptor, bool) {
	if v == nil {
		return descriptor.Descriptor{}, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if len(v.regionIndex) == 0 {
		return descriptor.Descriptor{}, false
	}
	idx := sort.Search(len(v.regionIndex), func(i int) bool {
		return bytes.Compare(v.regionIndex[i].start, key) > 0
	})
	if idx == 0 {
		return descriptor.Descriptor{}, false
	}
	entry := v.regionIndex[idx-1]
	if bytes.Compare(key, entry.start) < 0 {
		return descriptor.Descriptor{}, false
	}
	if len(entry.end) > 0 && bytes.Compare(key, entry.end) >= 0 {
		return descriptor.Descriptor{}, false
	}
	desc, ok := v.regions[entry.id]
	if !ok {
		return descriptor.Descriptor{}, false
	}
	return desc.Clone(), true
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

func (v *RegionDirectoryView) findOverlapLocked(desc descriptor.Descriptor) (uint64, bool) {
	for id, existing := range v.regions {
		if id == desc.RegionID {
			continue
		}
		if rangesOverlap(desc, existing) {
			return id, true
		}
	}
	return 0, false
}

func (v *RegionDirectoryView) rebuildIndexLocked() {
	index := make([]regionIndexEntry, 0, len(v.regions))
	for id, desc := range v.regions {
		index = append(index, regionIndexEntry{id: id, start: append([]byte(nil), desc.StartKey...), end: append([]byte(nil), desc.EndKey...)})
	}
	sort.Slice(index, func(i, j int) bool {
		if cmp := bytes.Compare(index[i].start, index[j].start); cmp != 0 {
			return cmp < 0
		}
		return index[i].id < index[j].id
	})
	v.regionIndex = index
}

func isEpochStale(incoming, current metaregion.Epoch) bool {
	if incoming.Version < current.Version {
		return true
	}
	if incoming.Version == current.Version && incoming.ConfVersion < current.ConfVersion {
		return true
	}
	return false
}

func rangesOverlap(a, b descriptor.Descriptor) bool {
	if len(a.EndKey) > 0 && bytes.Compare(a.EndKey, b.StartKey) <= 0 {
		return false
	}
	if len(b.EndKey) > 0 && bytes.Compare(b.EndKey, a.StartKey) <= 0 {
		return false
	}
	return true
}
