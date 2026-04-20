package view

import (
	"sort"
	"sync"
	"time"
)

// StoreStats captures store-level heartbeat data tracked by Coordinator views.
type StoreStats struct {
	StoreID           uint64    `json:"store_id"`
	RegionNum         uint64    `json:"region_num"`
	LeaderNum         uint64    `json:"leader_num"`
	Capacity          uint64    `json:"capacity"`
	Available         uint64    `json:"available"`
	DroppedOperations uint64    `json:"dropped_operations"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// StoreHealthView is the disposable control-plane view of store heartbeats.
type StoreHealthView struct {
	mu     sync.RWMutex
	stores map[uint64]StoreStats
}

func NewStoreHealthView() *StoreHealthView {
	return &StoreHealthView{stores: make(map[uint64]StoreStats)}
}

func (v *StoreHealthView) Upsert(stats StoreStats) error {
	return v.UpsertAt(stats, time.Now())
}

func (v *StoreHealthView) UpsertAt(stats StoreStats, now time.Time) error {
	if v == nil {
		return nil
	}
	if stats.StoreID == 0 {
		return ErrInvalidStoreID
	}
	stats.UpdatedAt = now
	v.mu.Lock()
	v.stores[stats.StoreID] = stats
	v.mu.Unlock()
	return nil
}

func (v *StoreHealthView) Remove(storeID uint64) {
	if v == nil || storeID == 0 {
		return
	}
	v.mu.Lock()
	delete(v.stores, storeID)
	v.mu.Unlock()
}

func (v *StoreHealthView) Snapshot() []StoreStats {
	if v == nil {
		return nil
	}
	v.mu.RLock()
	out := make([]StoreStats, 0, len(v.stores))
	for _, st := range v.stores {
		out = append(out, st)
	}
	v.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].StoreID < out[j].StoreID })
	return out
}
