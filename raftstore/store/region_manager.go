package store

import (
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type regionManager struct {
	mu       sync.RWMutex
	metaByID map[uint64]manifest.RegionMeta
	peers    map[uint64]*peer.Peer
	manifest *manifest.Manager
	hooks    RegionHooks
}

func newRegionManager(man *manifest.Manager, hooks RegionHooks) *regionManager {
	return &regionManager{
		metaByID: make(map[uint64]manifest.RegionMeta),
		peers:    make(map[uint64]*peer.Peer),
		manifest: man,
		hooks:    hooks,
	}
}

func (rm *regionManager) loadSnapshot(snapshot map[uint64]manifest.RegionMeta) {
	if rm == nil || len(snapshot) == 0 {
		return
	}
	rm.mu.Lock()
	for id, meta := range snapshot {
		rm.metaByID[id] = manifest.CloneRegionMeta(meta)
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

func (rm *regionManager) meta(regionID uint64) (manifest.RegionMeta, bool) {
	if rm == nil || regionID == 0 {
		return manifest.RegionMeta{}, false
	}
	rm.mu.RLock()
	meta, ok := rm.metaByID[regionID]
	rm.mu.RUnlock()
	if !ok {
		return manifest.RegionMeta{}, false
	}
	return manifest.CloneRegionMeta(meta), true
}

func (rm *regionManager) listMetas() []manifest.RegionMeta {
	if rm == nil {
		return nil
	}
	rm.mu.RLock()
	out := make([]manifest.RegionMeta, 0, len(rm.metaByID))
	for _, meta := range rm.metaByID {
		out = append(out, manifest.CloneRegionMeta(meta))
	}
	rm.mu.RUnlock()
	return out
}

func (rm *regionManager) updateRegion(meta manifest.RegionMeta) error {
	if rm == nil {
		return fmt.Errorf("raftstore: region manager nil")
	}
	if meta.ID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	metaCopy := manifest.CloneRegionMeta(meta)
	if metaCopy.State == 0 {
		metaCopy.State = manifest.RegionStateRunning
	}

	var currentState manifest.RegionState
	rm.mu.RLock()
	if existing, ok := rm.metaByID[metaCopy.ID]; ok {
		currentState = existing.State
	} else {
		currentState = manifest.RegionStateNew
	}
	rm.mu.RUnlock()

	if !validRegionStateTransition(currentState, metaCopy.State) {
		return fmt.Errorf("raftstore: invalid region %d state transition %v -> %v", metaCopy.ID, currentState, metaCopy.State)
	}

	if rm.manifest != nil {
		if err := rm.manifest.LogRegionUpdate(metaCopy); err != nil {
			return err
		}
	}

	rm.mu.Lock()
	rm.metaByID[metaCopy.ID] = manifest.CloneRegionMeta(metaCopy)
	p := rm.peers[metaCopy.ID]
	rm.mu.Unlock()

	if p != nil {
		p.SetRegionMeta(metaCopy)
	}
	if rm.hooks.OnRegionUpdate != nil {
		rm.hooks.OnRegionUpdate(metaCopy)
	}
	return nil
}

func (rm *regionManager) updateRegionState(regionID uint64, state manifest.RegionState) error {
	if rm == nil {
		return fmt.Errorf("raftstore: region manager nil")
	}
	meta, ok := rm.meta(regionID)
	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}
	meta.State = state
	return rm.updateRegion(meta)
}

func (rm *regionManager) removeRegion(regionID uint64) error {
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
	if meta.State != manifest.RegionStateTombstone {
		meta.State = manifest.RegionStateTombstone
		if err := rm.updateRegion(meta); err != nil {
			return err
		}
	}
	if rm.manifest != nil {
		if err := rm.manifest.LogRegionDelete(regionID); err != nil {
			return err
		}
	}
	rm.mu.Lock()
	delete(rm.metaByID, regionID)
	delete(rm.peers, regionID)
	rm.mu.Unlock()
	if rm.hooks.OnRegionRemove != nil {
		rm.hooks.OnRegionRemove(regionID)
	}
	return nil
}

func validRegionStateTransition(current, next manifest.RegionState) bool {
	if current == next {
		return true
	}
	switch current {
	case manifest.RegionStateNew:
		return next == manifest.RegionStateRunning
	case manifest.RegionStateRunning:
		return next == manifest.RegionStateRemoving || next == manifest.RegionStateTombstone
	case manifest.RegionStateRemoving:
		return next == manifest.RegionStateTombstone
	case manifest.RegionStateTombstone:
		return next == manifest.RegionStateTombstone
	default:
		return false
	}
}
