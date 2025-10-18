package peer

import (
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
)

func cloneRegionMeta(meta *manifest.RegionMeta) *manifest.RegionMeta {
    if meta == nil {
        return nil
    }
    cp := *meta
    cp.StartKey = append([]byte(nil), meta.StartKey...)
    cp.EndKey = append([]byte(nil), meta.EndKey...)
    cp.Peers = append([]manifest.PeerMeta(nil), meta.Peers...)
    return &cp
}

type RegionManager struct {
    mu         sync.RWMutex
    peers      map[uint64]*Peer
    catalog    map[uint64]manifest.RegionMeta
}

func NewRegionManager() *RegionManager {
    return &RegionManager{
        peers:   make(map[uint64]*Peer),
        catalog: make(map[uint64]manifest.RegionMeta),
    }
}

func (rm *RegionManager) Add(meta manifest.RegionMeta, p *Peer) error {
    if p == nil {
        return fmt.Errorf("region manager: peer is nil")
    }
    rm.mu.Lock()
    defer rm.mu.Unlock()
    if _, exists := rm.peers[meta.ID]; exists {
        return fmt.Errorf("region manager: region %d already exists", meta.ID)
    }
    rm.peers[meta.ID] = p
    rm.catalog[meta.ID] = meta
    return nil
}

func (rm *RegionManager) Remove(regionID uint64) {
    rm.mu.Lock()
    defer rm.mu.Unlock()
    delete(rm.peers, regionID)
    delete(rm.catalog, regionID)
}

func (rm *RegionManager) Get(regionID uint64) (*Peer, bool) {
    rm.mu.RLock()
    defer rm.mu.RUnlock()
    p, ok := rm.peers[regionID]
    return p, ok
}

func (rm *RegionManager) RegionMeta(regionID uint64) (manifest.RegionMeta, bool) {
    rm.mu.RLock()
    defer rm.mu.RUnlock()
    meta, ok := rm.catalog[regionID]
    return meta, ok
}

func (rm *RegionManager) Regions() []uint64 {
    rm.mu.RLock()
    defer rm.mu.RUnlock()
    ids := make([]uint64, 0, len(rm.peers))
    for id := range rm.peers {
        ids = append(ids, id)
    }
    return ids
}
