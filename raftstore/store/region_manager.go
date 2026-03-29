package store

import (
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/metrics"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type regionManager struct {
	mu            sync.RWMutex
	metaByID      map[uint64]raftmeta.RegionMeta
	peers         map[uint64]*peer.Peer
	localMeta     *raftmeta.Store
	regionMetrics *metrics.RegionMetrics
	notify        func(regionEvent)
}

func newRegionManager(localMeta *raftmeta.Store, regionMetrics *metrics.RegionMetrics, notify func(regionEvent)) *regionManager {
	return &regionManager{
		metaByID:      make(map[uint64]raftmeta.RegionMeta),
		peers:         make(map[uint64]*peer.Peer),
		localMeta:     localMeta,
		regionMetrics: regionMetrics,
		notify:        notify,
	}
}

func (rm *regionManager) loadBootstrapSnapshot(snapshot map[uint64]raftmeta.RegionMeta) {
	if rm == nil || len(snapshot) == 0 {
		return
	}
	rm.mu.Lock()
	for id, meta := range snapshot {
		metaCopy := raftmeta.CloneRegionMeta(meta)
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

func (rm *regionManager) meta(regionID uint64) (raftmeta.RegionMeta, bool) {
	if rm == nil || regionID == 0 {
		return raftmeta.RegionMeta{}, false
	}
	rm.mu.RLock()
	meta, ok := rm.metaByID[regionID]
	rm.mu.RUnlock()
	if !ok {
		return raftmeta.RegionMeta{}, false
	}
	return raftmeta.CloneRegionMeta(meta), true
}

func (rm *regionManager) listMetas() []raftmeta.RegionMeta {
	if rm == nil {
		return nil
	}
	rm.mu.RLock()
	out := make([]raftmeta.RegionMeta, 0, len(rm.metaByID))
	for _, meta := range rm.metaByID {
		out = append(out, raftmeta.CloneRegionMeta(meta))
	}
	rm.mu.RUnlock()
	return out
}

func (rm *regionManager) applyRegionMeta(meta raftmeta.RegionMeta) error {
	if rm == nil {
		return fmt.Errorf("raftstore: region manager nil")
	}
	if meta.ID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	metaCopy := raftmeta.CloneRegionMeta(meta)
	if metaCopy.State == 0 {
		metaCopy.State = raftmeta.RegionStateRunning
	}

	var currentState raftmeta.RegionState
	rm.mu.RLock()
	if existing, ok := rm.metaByID[metaCopy.ID]; ok {
		currentState = existing.State
	} else {
		currentState = raftmeta.RegionStateNew
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
	rm.metaByID[metaCopy.ID] = raftmeta.CloneRegionMeta(metaCopy)
	p := rm.peers[metaCopy.ID]
	rm.mu.Unlock()

	if p != nil {
		p.SetRegionMeta(metaCopy)
	}
	if rm.regionMetrics != nil {
		rm.regionMetrics.RecordUpdate(metaCopy)
	}
	if rm.notify != nil {
		rm.notify(regionEvent{
			kind:     regionEventApply,
			regionID: metaCopy.ID,
			meta:     metaCopy,
		})
	}
	return nil
}

func (rm *regionManager) applyRegionState(regionID uint64, state raftmeta.RegionState) error {
	if rm == nil {
		return fmt.Errorf("raftstore: region manager nil")
	}
	meta, ok := rm.meta(regionID)
	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}
	meta.State = state
	return rm.applyRegionMeta(meta)
}

func (rm *regionManager) applyRegionRemoval(regionID uint64) error {
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
	if meta.State != raftmeta.RegionStateTombstone {
		meta.State = raftmeta.RegionStateTombstone
		if err := rm.applyRegionMeta(meta); err != nil {
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
	if rm.notify != nil {
		rm.notify(regionEvent{
			kind:     regionEventRemove,
			regionID: regionID,
		})
	}
	return nil
}

func validRegionStateTransition(current, next raftmeta.RegionState) bool {
	if current == next {
		return true
	}
	switch current {
	case raftmeta.RegionStateNew:
		return next == raftmeta.RegionStateRunning
	case raftmeta.RegionStateRunning:
		return next == raftmeta.RegionStateRemoving || next == raftmeta.RegionStateTombstone
	case raftmeta.RegionStateRemoving:
		return next == raftmeta.RegionStateTombstone
	case raftmeta.RegionStateTombstone:
		return next == raftmeta.RegionStateTombstone
	default:
		return false
	}
}
