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
	// LeaderStoreID is the store that most recently reported itself as the
	// raft leader of this region via StoreHeartbeat. Zero means no store
	// has claimed leadership yet (or all claims are stale).
	LeaderStoreID    uint64    `json:"leader_store_id,omitempty"`
	LeaderReportedAt time.Time `json:"leader_reported_at"`
}

type regionIndexEntry struct {
	id    uint64
	start []byte
	end   []byte
}

type regionLeader struct {
	storeID    uint64
	reportedAt time.Time
}

// RegionDirectoryView is the disposable control-plane directory view used for
// route lookup and operator inspection.
type RegionDirectoryView struct {
	mu            sync.RWMutex
	regions       map[uint64]descriptor.Descriptor
	regionLastHB  map[uint64]time.Time
	regionLeaders map[uint64]regionLeader
	regionIndex   []regionIndexEntry
}

func NewRegionDirectoryView() *RegionDirectoryView {
	return &RegionDirectoryView{
		regions:       make(map[uint64]descriptor.Descriptor),
		regionLastHB:  make(map[uint64]time.Time),
		regionLeaders: make(map[uint64]regionLeader),
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
	entry := regionIndexEntry{id: desc.RegionID, start: append([]byte(nil), desc.StartKey...), end: append([]byte(nil), desc.EndKey...)}
	restoreAt := -1
	var restore regionIndexEntry
	if exists {
		restoreAt = v.removeIndexByRegionIDLocked(desc.RegionID)
		if restoreAt >= 0 {
			restore = regionIndexEntry{
				id:    current.RegionID,
				start: append([]byte(nil), current.StartKey...),
				end:   append([]byte(nil), current.EndKey...),
			}
		}
	}
	insertAt, overlapID, overlaps := v.validateInsertLocked(entry)
	if overlaps {
		if restoreAt >= 0 {
			v.insertIndexLocked(restoreAt, restore)
		}
		return fmt.Errorf("%w: region=%d overlaps region=%d", ErrRegionRangeOverlap, desc.RegionID, overlapID)
	}
	v.regions[desc.RegionID] = desc.Clone()
	v.regionLastHB[desc.RegionID] = now
	v.insertIndexLocked(insertAt, entry)
	return nil
}

// Validate checks whether desc can be inserted into the current directory view
// without mutating tracked regions or index state.
func (v *RegionDirectoryView) Validate(desc descriptor.Descriptor) error {
	if v == nil {
		return nil
	}
	if desc.RegionID == 0 {
		return ErrInvalidRegionID
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	current, exists := v.regions[desc.RegionID]
	if exists && isEpochStale(desc.Epoch, current.Epoch) {
		return fmt.Errorf("%w: region=%d current={ver:%d conf:%d} incoming={ver:%d conf:%d}",
			ErrRegionHeartbeatStale,
			desc.RegionID,
			current.Epoch.Version, current.Epoch.ConfVersion,
			desc.Epoch.Version, desc.Epoch.ConfVersion,
		)
	}
	entry := regionIndexEntry{id: desc.RegionID, start: desc.StartKey, end: desc.EndKey}
	if overlapID, ok := v.findOverlapForEntryLocked(entry); ok {
		return fmt.Errorf("%w: region=%d overlaps region=%d", ErrRegionRangeOverlap, desc.RegionID, overlapID)
	}
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
	delete(v.regionLeaders, regionID)
	v.removeIndexByRegionIDLocked(regionID)
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
		info := RegionInfo{Descriptor: desc.Clone(), LastHeartbeat: v.regionLastHB[id]}
		if leader, ok := v.regionLeaders[id]; ok {
			info.LeaderStoreID = leader.storeID
			info.LeaderReportedAt = leader.reportedAt
		}
		out = append(out, info)
	}
	v.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Descriptor.RegionID < out[j].Descriptor.RegionID })
	return out
}

// RecordLeader updates the most-recent per-region leader claim observed
// via StoreHeartbeat. Latest report wins; during a raft transition the
// two claims will converge within one heartbeat tick.
func (v *RegionDirectoryView) RecordLeader(regionID, storeID uint64, at time.Time) {
	if v == nil || regionID == 0 || storeID == 0 {
		return
	}
	v.mu.Lock()
	v.regionLeaders[regionID] = regionLeader{storeID: storeID, reportedAt: at}
	v.mu.Unlock()
}

// ClearLeadersFromStore removes any leadership claim owned by storeID.
// Used on StoreHeartbeat from a store that no longer claims leadership
// over a previously-owned region (the store stopped being leader).
func (v *RegionDirectoryView) ClearLeadersFromStore(storeID uint64, keep map[uint64]struct{}) {
	if v == nil || storeID == 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for regionID, leader := range v.regionLeaders {
		if leader.storeID != storeID {
			continue
		}
		if _, keepIt := keep[regionID]; keepIt {
			continue
		}
		delete(v.regionLeaders, regionID)
	}
}

func (v *RegionDirectoryView) DescriptorsSnapshot() map[uint64]descriptor.Descriptor {
	if v == nil {
		return make(map[uint64]descriptor.Descriptor)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make(map[uint64]descriptor.Descriptor, len(v.regions))
	for id, desc := range v.regions {
		out[id] = desc.Clone()
	}
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
	prunedLeaders := make(map[uint64]regionLeader, len(regions))
	for id, leader := range v.regionLeaders {
		if _, ok := regions[id]; ok {
			prunedLeaders[id] = leader
		}
	}
	v.regionLeaders = prunedLeaders
	v.regionIndex = buildRegionIndex(regions)
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

func (v *RegionDirectoryView) Descriptor(regionID uint64) (descriptor.Descriptor, bool) {
	if v == nil || regionID == 0 {
		return descriptor.Descriptor{}, false
	}
	v.mu.RLock()
	desc, ok := v.regions[regionID]
	v.mu.RUnlock()
	if !ok {
		return descriptor.Descriptor{}, false
	}
	return desc.Clone(), true
}

func (v *RegionDirectoryView) Touch(regionID uint64, now time.Time) bool {
	if v == nil || regionID == 0 {
		return false
	}
	v.mu.Lock()
	_, ok := v.regions[regionID]
	if ok {
		v.regionLastHB[regionID] = now
	}
	v.mu.Unlock()
	return ok
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

func (v *RegionDirectoryView) validateInsertLocked(entry regionIndexEntry) (int, uint64, bool) {
	insertAt := searchRegionIndex(v.regionIndex, entry.start, entry.id)
	if overlapID, ok := v.findOverlapForEntryAtLocked(entry, insertAt); ok {
		return 0, overlapID, true
	}
	return insertAt, 0, false
}

func (v *RegionDirectoryView) findOverlapForEntryLocked(entry regionIndexEntry) (uint64, bool) {
	insertAt := searchRegionIndex(v.regionIndex, entry.start, entry.id)
	return v.findOverlapForEntryAtLocked(entry, insertAt)
}

func (v *RegionDirectoryView) findOverlapForEntryAtLocked(entry regionIndexEntry, insertAt int) (uint64, bool) {
	if insertAt > 0 {
		prev := v.regionIndex[insertAt-1]
		if prev.id != entry.id && indexEntriesOverlap(prev, entry) {
			return prev.id, true
		}
	}
	if insertAt < len(v.regionIndex) {
		next := v.regionIndex[insertAt]
		if next.id != entry.id && indexEntriesOverlap(entry, next) {
			return next.id, true
		}
	}
	return 0, false
}

func (v *RegionDirectoryView) removeIndexByRegionIDLocked(regionID uint64) int {
	for i, entry := range v.regionIndex {
		if entry.id != regionID {
			continue
		}
		v.regionIndex = append(v.regionIndex[:i], v.regionIndex[i+1:]...)
		return i
	}
	return -1
}

func (v *RegionDirectoryView) insertIndexLocked(at int, entry regionIndexEntry) {
	if at < 0 || at > len(v.regionIndex) {
		at = len(v.regionIndex)
	}
	v.regionIndex = append(v.regionIndex, regionIndexEntry{})
	copy(v.regionIndex[at+1:], v.regionIndex[at:])
	v.regionIndex[at] = entry
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

func buildRegionIndex(regions map[uint64]descriptor.Descriptor) []regionIndexEntry {
	index := make([]regionIndexEntry, 0, len(regions))
	for id, desc := range regions {
		index = append(index, regionIndexEntry{
			id:    id,
			start: append([]byte(nil), desc.StartKey...),
			end:   append([]byte(nil), desc.EndKey...),
		})
	}
	sort.Slice(index, func(i, j int) bool {
		if cmp := bytes.Compare(index[i].start, index[j].start); cmp != 0 {
			return cmp < 0
		}
		return index[i].id < index[j].id
	})
	return index
}

func searchRegionIndex(index []regionIndexEntry, start []byte, id uint64) int {
	return sort.Search(len(index), func(i int) bool {
		if cmp := bytes.Compare(index[i].start, start); cmp != 0 {
			return cmp > 0
		}
		return index[i].id >= id
	})
}

func indexEntriesOverlap(a, b regionIndexEntry) bool {
	if len(a.end) > 0 && bytes.Compare(a.end, b.start) <= 0 {
		return false
	}
	if len(b.end) > 0 && bytes.Compare(b.end, a.start) <= 0 {
		return false
	}
	return true
}
