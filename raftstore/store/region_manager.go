package store

import (
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type regionManager struct {
	mu       sync.RWMutex
	metaByID map[uint64]manifest.RegionMeta
	peers    map[uint64]*peer.Peer
}

func newRegionManager() *regionManager {
	return &regionManager{
		metaByID: make(map[uint64]manifest.RegionMeta),
		peers:    make(map[uint64]*peer.Peer),
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

func (rm *regionManager) updateMeta(meta manifest.RegionMeta) *peer.Peer {
	if rm == nil || meta.ID == 0 {
		return nil
	}
	rm.mu.Lock()
	rm.metaByID[meta.ID] = manifest.CloneRegionMeta(meta)
	p := rm.peers[meta.ID]
	rm.mu.Unlock()
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

func (rm *regionManager) remove(regionID uint64) {
	if rm == nil || regionID == 0 {
		return
	}
	rm.mu.Lock()
	delete(rm.metaByID, regionID)
	delete(rm.peers, regionID)
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
