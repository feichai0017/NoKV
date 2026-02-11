package store

import (
	"fmt"

	"github.com/feichai0017/NoKV/manifest"
)

// UpdateRegion persists the region metadata (when a manifest manager is
// configured) and updates the in-memory catalog plus the running peer's
// snapshot, if any. Callers can use this to refresh epoch information,
// peer memberships, or lifecycle state transitions.
func (s *Store) UpdateRegion(meta manifest.RegionMeta) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regions.updateRegion(meta)
}

// RemoveRegion tombstones the region metadata from the manifest (when present)
// and evicts it from the in-memory catalog. It is intended to be invoked after
// the corresponding peer has been stopped.
func (s *Store) RemoveRegion(regionID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regions.removeRegion(regionID)
}

// UpdateRegionState loads the currently known metadata and advances the state
// machine to the requested value (Running/Removing/Tombstone) while validating
// legal transitions.
func (s *Store) UpdateRegionState(regionID uint64, state manifest.RegionState) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regions.updateRegionState(regionID, state)
}

// RegionMetas collects the known manifest.RegionMeta entries from registered
// peers. This mirrors the TinyKV store exposing region layout information to
// schedulers and debugging endpoints.
func (s *Store) RegionMetas() []manifest.RegionMeta {
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
func (s *Store) RegionMetaByID(regionID uint64) (manifest.RegionMeta, bool) {
	if s == nil || regionID == 0 {
		return manifest.RegionMeta{}, false
	}
	if s.regions == nil {
		return manifest.RegionMeta{}, false
	}
	return s.regions.meta(regionID)
}

// RegionSnapshot returns a snapshot containing all region metadata currently
// known to the store. The resulting slice is safe for callers to modify.
func (s *Store) RegionSnapshot() RegionSnapshot {
	return RegionSnapshot{Regions: s.RegionMetas()}
}

// RegionMetrics returns the metrics recorder tracking region state counts.
func (s *Store) RegionMetrics() *RegionMetrics {
	if s == nil {
		return nil
	}
	return s.regionMetrics
}
