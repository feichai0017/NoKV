package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/metrics"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type regionManager struct {
	mu            sync.RWMutex
	ctx           context.Context
	metaByID      map[uint64]raftmeta.RegionMeta
	peers         map[uint64]*peer.Peer
	localMeta     *raftmeta.Store
	regionMetrics *metrics.RegionMetrics
	scheduler     SchedulerClient
}

func newRegionManager(ctx context.Context, localMeta *raftmeta.Store, regionMetrics *metrics.RegionMetrics, scheduler SchedulerClient) *regionManager {
	return &regionManager{
		ctx:           ctx,
		metaByID:      make(map[uint64]raftmeta.RegionMeta),
		peers:         make(map[uint64]*peer.Peer),
		localMeta:     localMeta,
		regionMetrics: regionMetrics,
		scheduler:     scheduler,
	}
}

func (rm *regionManager) loadSnapshot(snapshot map[uint64]raftmeta.RegionMeta) {
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

func (rm *regionManager) updateRegion(meta raftmeta.RegionMeta) error {
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
	if rm.scheduler != nil {
		rm.scheduler.PublishRegion(rm.runtimeContext(), metaCopy)
	}
	return nil
}

func (rm *regionManager) updateRegionState(regionID uint64, state raftmeta.RegionState) error {
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
	if meta.State != raftmeta.RegionStateTombstone {
		meta.State = raftmeta.RegionStateTombstone
		if err := rm.updateRegion(meta); err != nil {
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
	if rm.scheduler != nil {
		rm.scheduler.RemoveRegion(rm.runtimeContext(), regionID)
	}
	return nil
}

func (rm *regionManager) runtimeContext() context.Context {
	if rm == nil || rm.ctx == nil {
		return context.Background()
	}
	return rm.ctx
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
