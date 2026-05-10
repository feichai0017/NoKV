package store

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"

	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/raftstore/store/region"
)

// RegionSnapshot, PeerHandle, and RegionRuntimeStatus are public facades over
// the region subpackage's types. Aliasing keeps the Store's existing public
// API stable while the implementation lives in raftstore/store/region.
type (
	RegionSnapshot      = region.Snapshot
	PeerHandle          = region.PeerHandle
	RegionRuntimeStatus = region.RuntimeStatus
)

// enqueueRegionRootEvent is the bridge from the region.Manager notify hook
// to the scheduler runtime's regionEvent queue.
func (s *Store) enqueueRegionRootEvent(ev rootevent.Event) {
	if s == nil {
		return
	}
	s.enqueueRegionEvent(regionEvent{root: ev})
}

func (s *Store) applyRegionMeta(meta localmeta.RegionMeta) error {
	if s == nil {
		return errNilStore
	}
	return s.regions.Apply(meta, true)
}

func (s *Store) applyRegionMetaSilent(meta localmeta.RegionMeta) error {
	if s == nil {
		return errNilStore
	}
	return s.regions.Apply(meta, false)
}

func (s *Store) applyRegionRemoval(regionID uint64) error {
	if s == nil {
		return errNilStore
	}
	return s.regions.Remove(regionID, true)
}

func (s *Store) applyRegionRemovalSilent(regionID uint64) error {
	if s == nil {
		return errNilStore
	}
	return s.regions.Remove(regionID, false)
}

func (s *Store) applyRegionState(regionID uint64, state metaregion.ReplicaState) error {
	if s == nil {
		return errNilStore
	}
	return s.regions.ApplyState(regionID, state)
}

// RegionMetas collects the known localmeta.RegionMeta entries from registered
// peers. This mirrors the TinyKV store exposing region layout information to
// schedulers and debugging endpoints.
func (s *Store) RegionMetas() []localmeta.RegionMeta {
	if s == nil || s.regions == nil {
		return nil
	}
	return s.regions.Metas()
}

// RegionMetaByID returns the stored metadata for the provided region, along
// with a boolean indicating whether it exists.
func (s *Store) RegionMetaByID(regionID uint64) (localmeta.RegionMeta, bool) {
	if s == nil || regionID == 0 || s.regions == nil {
		return localmeta.RegionMeta{}, false
	}
	return s.regions.Meta(regionID)
}

// RegionMetaByKey returns the stored region metadata that owns key.
func (s *Store) RegionMetaByKey(key []byte) (localmeta.RegionMeta, bool) {
	if s == nil || len(key) == 0 || s.regions == nil {
		return localmeta.RegionMeta{}, false
	}
	return s.regions.MetaByKey(key)
}

// RegionSnapshot returns a snapshot containing all region metadata currently
// known to the store. The resulting slice is safe for callers to modify.
func (s *Store) RegionSnapshot() RegionSnapshot {
	return RegionSnapshot{Regions: s.RegionMetas()}
}

// RegionRuntimeStatus returns the store-local runtime status for one region.
func (s *Store) RegionRuntimeStatus(regionID uint64) (RegionRuntimeStatus, bool) {
	if s == nil || s.regions == nil {
		return RegionRuntimeStatus{}, false
	}
	return s.regions.RuntimeStatus(regionID)
}

// RegionMetrics returns the metrics recorder tracking region state counts.
func (s *Store) RegionMetrics() *metrics.RegionMetrics {
	if s == nil || s.regions == nil {
		return nil
	}
	return s.regions.Metrics()
}
