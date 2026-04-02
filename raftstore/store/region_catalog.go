package store

import (
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"

	"github.com/feichai0017/NoKV/metrics"
)

// RegionRuntimeStatus captures store-local runtime state for one region.
type RegionRuntimeStatus struct {
	Meta         localmeta.RegionMeta
	Hosted       bool
	LocalPeerID  uint64
	LeaderPeerID uint64
	Leader       bool
	AppliedIndex uint64
	AppliedTerm  uint64
}

func (s *Store) applyRegionMeta(meta localmeta.RegionMeta) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regionMgr().applyRegionMeta(meta)
}

func (s *Store) applyRegionRemoval(regionID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regionMgr().applyRegionRemoval(regionID)
}

func (s *Store) applyRegionState(regionID uint64, state metaregion.ReplicaState) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	return s.regionMgr().applyRegionState(regionID, state)
}

func (s *Store) regionMgr() *regionManager {
	if s == nil || s.regions == nil {
		return nil
	}
	return s.regions.mgr
}

func (s *Store) regionMetrics() *metrics.RegionMetrics {
	if s == nil || s.regions == nil {
		return nil
	}
	return s.regions.metrics
}

// RegionMetas collects the known localmeta.RegionMeta entries from registered
// peers. This mirrors the TinyKV store exposing region layout information to
// schedulers and debugging endpoints.
func (s *Store) RegionMetas() []localmeta.RegionMeta {
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
func (s *Store) RegionMetaByID(regionID uint64) (localmeta.RegionMeta, bool) {
	if s == nil || regionID == 0 {
		return localmeta.RegionMeta{}, false
	}
	if s.regionMgr() == nil {
		return localmeta.RegionMeta{}, false
	}
	return s.regionMgr().meta(regionID)
}

// RegionSnapshot returns a snapshot containing all region metadata currently
// known to the store. The resulting slice is safe for callers to modify.
func (s *Store) RegionSnapshot() RegionSnapshot {
	return RegionSnapshot{Regions: s.RegionMetas()}
}

// RegionRuntimeStatus returns the store-local runtime status for one region.
func (s *Store) RegionRuntimeStatus(regionID uint64) (RegionRuntimeStatus, bool) {
	meta, ok := s.RegionMetaByID(regionID)
	if !ok {
		return RegionRuntimeStatus{}, false
	}
	status := RegionRuntimeStatus{Meta: meta}
	peerRef := s.regionMgr().peer(regionID)
	if peerRef == nil {
		return status, true
	}
	raftStatus := peerRef.Status()
	status.Hosted = true
	status.LocalPeerID = peerRef.ID()
	status.LeaderPeerID = raftStatus.Lead
	status.Leader = raftStatus.RaftState == myraft.StateLeader
	if rm := s.regionMgr(); rm != nil && rm.localMeta != nil {
		if ptr, ok := rm.localMeta.RaftPointer(regionID); ok {
			status.AppliedIndex = ptr.AppliedIndex
			status.AppliedTerm = ptr.AppliedTerm
		}
	}
	return status, true
}

// RegionMetrics returns the metrics recorder tracking region state counts.
func (s *Store) RegionMetrics() *metrics.RegionMetrics {
	if s == nil {
		return nil
	}
	return s.regionMetrics()
}
