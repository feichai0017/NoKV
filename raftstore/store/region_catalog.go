package store

import (
	"fmt"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"

	"github.com/feichai0017/NoKV/metrics"
)

// UpdateRegion persists the region metadata in the local peer catalog and
// updates the in-memory catalog plus the running peer snapshot, if any.
// This is a local catalog hook for bootstrap/tests; consensus-changing
// metadata must still flow through raft apply paths.
func (s *Store) UpdateRegion(meta raftmeta.RegionMeta) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regions.updateRegion(meta)
}

// RemoveRegion tombstones the region metadata from the local peer catalog and
// evicts it from the in-memory catalog. It is intended for local cleanup after
// the corresponding peer has already been stopped.
func (s *Store) RemoveRegion(regionID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regions.removeRegion(regionID)
}

// UpdateRegionState loads the currently known metadata and advances the local
// catalog state machine to the requested value (Running/Removing/Tombstone)
// while validating legal transitions.
func (s *Store) UpdateRegionState(regionID uint64, state raftmeta.RegionState) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regions.updateRegionState(regionID, state)
}

// LoadRegionSnapshot replaces the in-memory region snapshot from a trusted
// bootstrap source such as local metadata recovery. It is intended for startup
// and tests, not arbitrary runtime mutation.
func (s *Store) LoadRegionSnapshot(snapshot map[uint64]raftmeta.RegionMeta) {
	if s == nil || s.regions == nil {
		return
	}
	s.regions.loadSnapshot(snapshot)
}

// RegionMetas collects the known raftmeta.RegionMeta entries from registered
// peers. This mirrors the TinyKV store exposing region layout information to
// schedulers and debugging endpoints.
func (s *Store) RegionMetas() []raftmeta.RegionMeta {
	if s == nil {
		return nil
	}
	if s.regions == nil {
		return nil
	}
	return s.regions.listMetas()
}

// RegionMetaByID returns the stored metadata for the provided region, along
// with a boolean indicating whether it exists.
func (s *Store) RegionMetaByID(regionID uint64) (raftmeta.RegionMeta, bool) {
	if s == nil || regionID == 0 {
		return raftmeta.RegionMeta{}, false
	}
	if s.regions == nil {
		return raftmeta.RegionMeta{}, false
	}
	return s.regions.meta(regionID)
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
	return s.regionMetrics
}
