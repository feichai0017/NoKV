package store

import (
	"fmt"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"

	"github.com/feichai0017/NoKV/metrics"
)

func (s *Store) updateRegion(meta raftmeta.RegionMeta) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regionMgr().updateRegion(meta)
}

func (s *Store) removeRegion(regionID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regionMgr().removeRegion(regionID)
}

func (s *Store) updateRegionState(regionID uint64, state raftmeta.RegionState) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regionMgr().updateRegionState(regionID, state)
}

// RegionMetas collects the known raftmeta.RegionMeta entries from registered
// peers. This mirrors the TinyKV store exposing region layout information to
// schedulers and debugging endpoints.
func (s *Store) RegionMetas() []raftmeta.RegionMeta {
	if s == nil {
		return nil
	}
	if s.regionMgr() == nil {
		return nil
	}
	return s.regionMgr().listMetas()
}

// RegionMetaByID returns the stored metadata for the provided region, along
// with a boolean indicating whether it exists.
func (s *Store) RegionMetaByID(regionID uint64) (raftmeta.RegionMeta, bool) {
	if s == nil || regionID == 0 {
		return raftmeta.RegionMeta{}, false
	}
	if s.regionMgr() == nil {
		return raftmeta.RegionMeta{}, false
	}
	return s.regionMgr().meta(regionID)
}

// RegionSnapshot returns a snapshot containing all region metadata currently
// known to the store. The resulting slice is safe for callers to modify.
func (s *Store) RegionSnapshot() RegionSnapshot {
	return RegionSnapshot{Regions: s.RegionMetas()}
}

// RegionMetrics returns the metrics recorder tracking region state counts.
func (s *Store) RegionMetrics() *metrics.RegionMetrics {
	if s == nil {
		return nil
	}
	return s.regionMetrics()
}
