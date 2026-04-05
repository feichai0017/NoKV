package store

import (
	"fmt"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"sync"

	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type regionManager struct {
	mu            sync.RWMutex
	metaByID      map[uint64]localmeta.RegionMeta
	peers         map[uint64]*peer.Peer
	localMeta     *localmeta.Store
	regionMetrics *metrics.RegionMetrics
	notify        func(regionEvent)
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
			rm.regionMetrics.RecordUpdate(metaCopy)
		}
	}
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

func (rm *regionManager) applyRegionMeta(meta localmeta.RegionMeta, publish bool) error {
	if rm == nil {
		return fmt.Errorf("raftstore: region manager nil")
	}
	if meta.ID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
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
		if err := rm.localMeta.SaveRegion(metaCopy); err != nil {
			return err
		}
	}

	rm.mu.Lock()
	_, existed := rm.metaByID[metaCopy.ID]
	rm.metaByID[metaCopy.ID] = localmeta.CloneRegionMeta(metaCopy)
	p := rm.peers[metaCopy.ID]
	rm.mu.Unlock()

	rm.syncPeerMirror(p, metaCopy)
	if rm.regionMetrics != nil {
		rm.regionMetrics.RecordUpdate(metaCopy)
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
		return fmt.Errorf("raftstore: region manager nil")
	}
	meta, ok := rm.meta(regionID)
	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}
	meta.State = state
	return rm.applyRegionMeta(meta, true)
}

func (rm *regionManager) applyRegionRemoval(regionID uint64, publish bool) error {
	if rm == nil {
		return fmt.Errorf("raftstore: region manager nil")
	}
	if regionID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	meta, ok := rm.meta(regionID)
	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
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

func catalogApplyRootEvent(meta localmeta.RegionMeta, existed bool) rootevent.Event {
	desc := metacodec.DescriptorFromLocalRegionMeta(meta, 0)
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
