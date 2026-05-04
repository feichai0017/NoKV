package store

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type regionManager struct {
	mu            sync.RWMutex
	metaByID      map[uint64]localmeta.RegionMeta
	metaByStart   []regionStartIndexEntry
	peers         map[uint64]*peer.Peer
	localMeta     *localmeta.Store
	regionMetrics *metrics.RegionMetrics
	notify        func(regionEvent)
}

type regionStartIndexEntry struct {
	start []byte
	id    uint64
}

// syncPeerMirror updates a peer's in-memory region snapshot after the local
// region truth has already been persisted and applied to the catalog.
func (rm *regionManager) syncPeerMirror(p *peer.Peer, meta localmeta.RegionMeta) {
	if p == nil {
		return
	}
	p.ApplyRegionMetaMirror(meta)
}

func newRegionManager(localMeta *localmeta.Store, regionMetrics *metrics.RegionMetrics, notify func(regionEvent)) *regionManager {
	return &regionManager{
		metaByID:      make(map[uint64]localmeta.RegionMeta),
		peers:         make(map[uint64]*peer.Peer),
		localMeta:     localMeta,
		regionMetrics: regionMetrics,
		notify:        notify,
	}
}

func (rm *regionManager) loadBootstrapSnapshot(snapshot map[uint64]localmeta.RegionMeta) {
	if rm == nil || len(snapshot) == 0 {
		return
	}
	rm.mu.Lock()
	for id, meta := range snapshot {
		metaCopy := localmeta.CloneRegionMeta(meta)
		rm.metaByID[id] = metaCopy
		if rm.regionMetrics != nil {
			rm.regionMetrics.RecordState(metaCopy.ID, metaCopy.State)
		}
	}
	rm.rebuildRangeIndexLocked()
	rm.mu.Unlock()
}

func (rm *regionManager) setPeer(regionID uint64, p *peer.Peer) {
	if rm == nil || regionID == 0 {
		return
	}
	rm.mu.Lock()
	if p == nil {
		delete(rm.peers, regionID)
	} else {
		rm.peers[regionID] = p
	}
	rm.mu.Unlock()
}

func (rm *regionManager) peer(regionID uint64) *peer.Peer {
	if rm == nil || regionID == 0 {
		return nil
	}
	rm.mu.RLock()
	p := rm.peers[regionID]
	rm.mu.RUnlock()
	return p
}

func (rm *regionManager) meta(regionID uint64) (localmeta.RegionMeta, bool) {
	if rm == nil || regionID == 0 {
		return localmeta.RegionMeta{}, false
	}
	rm.mu.RLock()
	meta, ok := rm.metaByID[regionID]
	rm.mu.RUnlock()
	if !ok {
		return localmeta.RegionMeta{}, false
	}
	return localmeta.CloneRegionMeta(meta), true
}

func (rm *regionManager) listMetas() []localmeta.RegionMeta {
	if rm == nil {
		return nil
	}
	rm.mu.RLock()
	out := make([]localmeta.RegionMeta, 0, len(rm.metaByID))
	for _, meta := range rm.metaByID {
		out = append(out, localmeta.CloneRegionMeta(meta))
	}
	rm.mu.RUnlock()
	return out
}

func (rm *regionManager) metaByKey(key []byte) (localmeta.RegionMeta, bool) {
	if rm == nil || len(key) == 0 {
		return localmeta.RegionMeta{}, false
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	idx := sort.Search(len(rm.metaByStart), func(i int) bool {
		return bytes.Compare(rm.metaByStart[i].start, key) > 0
	}) - 1
	if idx < 0 {
		return localmeta.RegionMeta{}, false
	}
	meta, ok := rm.metaByID[rm.metaByStart[idx].id]
	if !ok || !keyInRange(meta, key) {
		return localmeta.RegionMeta{}, false
	}
	return localmeta.CloneRegionMeta(meta), true
}

func (rm *regionManager) applyRegionMeta(meta localmeta.RegionMeta, publish bool) error {
	if rm == nil {
		return errRegionManagerNil
	}
	if meta.ID == 0 {
		return errZeroRegionID
	}
	metaCopy := localmeta.CloneRegionMeta(meta)
	if metaCopy.State == 0 {
		metaCopy.State = metaregion.ReplicaStateRunning
	}

	var currentState metaregion.ReplicaState
	rm.mu.RLock()
	if existing, ok := rm.metaByID[metaCopy.ID]; ok {
		currentState = existing.State
	} else {
		currentState = metaregion.ReplicaStateNew
	}
	rm.mu.RUnlock()

	if !validRegionStateTransition(currentState, metaCopy.State) {
		return fmt.Errorf("raftstore: invalid region %d state transition %v -> %v", metaCopy.ID, currentState, metaCopy.State)
	}

	if rm.localMeta != nil {
		// Local durable metadata is the restart source of truth. Persist it
		// before updating the in-memory mirror so crash recovery can always
		// rebuild the catalog from disk if this process dies in between.
		if err := rm.localMeta.SaveRegion(metaCopy); err != nil {
			return err
		}
	}

	rm.mu.Lock()
	_, existed := rm.metaByID[metaCopy.ID]
	rm.metaByID[metaCopy.ID] = localmeta.CloneRegionMeta(metaCopy)
	rm.rebuildRangeIndexLocked()
	p := rm.peers[metaCopy.ID]
	rm.mu.Unlock()

	rm.syncPeerMirror(p, metaCopy)
	if rm.regionMetrics != nil {
		rm.regionMetrics.RecordState(metaCopy.ID, metaCopy.State)
	}
	if publish && rm.notify != nil {
		rm.notify(regionEvent{
			root: catalogApplyRootEvent(metaCopy, existed),
		})
	}
	return nil
}

func (rm *regionManager) applyRegionState(regionID uint64, state metaregion.ReplicaState) error {
	if rm == nil {
		return errRegionManagerNil
	}
	meta, ok := rm.meta(regionID)
	if !ok {
		return errRegionNotFound(regionID)
	}
	meta.State = state
	return rm.applyRegionMeta(meta, true)
}

func (rm *regionManager) applyRegionRemoval(regionID uint64, publish bool) error {
	if rm == nil {
		return errRegionManagerNil
	}
	if regionID == 0 {
		return errZeroRegionID
	}
	meta, ok := rm.meta(regionID)
	if !ok {
		return errRegionNotFound(regionID)
	}
	if meta.State != metaregion.ReplicaStateTombstone {
		meta.State = metaregion.ReplicaStateTombstone
		if err := rm.applyRegionMeta(meta, publish); err != nil {
			return err
		}
	}
	if rm.localMeta != nil {
		if err := rm.localMeta.DeleteRegion(regionID); err != nil {
			return err
		}
	}
	rm.mu.Lock()
	delete(rm.metaByID, regionID)
	delete(rm.peers, regionID)
	rm.rebuildRangeIndexLocked()
	rm.mu.Unlock()
	if rm.regionMetrics != nil {
		rm.regionMetrics.RecordRemove(regionID)
	}
	if publish && rm.notify != nil {
		rm.notify(regionEvent{
			root: catalogRemovalRootEvent(regionID),
		})
	}
	return nil
}

func (rm *regionManager) rebuildRangeIndexLocked() {
	rm.metaByStart = rm.metaByStart[:0]
	for id, meta := range rm.metaByID {
		rm.metaByStart = append(rm.metaByStart, regionStartIndexEntry{
			start: append([]byte(nil), meta.StartKey...),
			id:    id,
		})
	}
	sort.Slice(rm.metaByStart, func(i, j int) bool {
		cmp := bytes.Compare(rm.metaByStart[i].start, rm.metaByStart[j].start)
		if cmp != 0 {
			return cmp < 0
		}
		return rm.metaByStart[i].id < rm.metaByStart[j].id
	})
}

func catalogApplyRootEvent(meta localmeta.RegionMeta, existed bool) rootevent.Event {
	desc := localmeta.Descriptor(meta, 0)
	if !existed {
		return rootevent.RegionBootstrapped(desc)
	}
	return rootevent.RegionDescriptorPublished(desc)
}

func catalogRemovalRootEvent(regionID uint64) rootevent.Event {
	return rootevent.RegionTombstoned(regionID)
}

func validRegionStateTransition(current, next metaregion.ReplicaState) bool {
	if current == next {
		return true
	}
	switch current {
	case metaregion.ReplicaStateNew:
		return next == metaregion.ReplicaStateRunning
	case metaregion.ReplicaStateRunning:
		return next == metaregion.ReplicaStateRemoving || next == metaregion.ReplicaStateTombstone
	case metaregion.ReplicaStateRemoving:
		return next == metaregion.ReplicaStateTombstone
	case metaregion.ReplicaStateTombstone:
		return next == metaregion.ReplicaStateTombstone
	default:
		return false
	}
}
