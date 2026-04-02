package storage

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"sync"
)

// RootStore persists PD truth on top of the metadata root and reconstructs the
// region catalog by replaying committed root events.
type RootStore struct {
	root rootpkg.Root

	mu       sync.RWMutex
	snapshot Snapshot
}

// OpenRootStore opens a PD storage backend backed by the metadata root.
func OpenRootStore(root rootpkg.Root) (*RootStore, error) {
	store := &RootStore{root: root}
	if err := store.reload(); err != nil {
		return nil, err
	}
	return store, nil
}

// Load returns the last reconstructed snapshot.
func (s *RootStore) Load() (Snapshot, error) {
	if s == nil {
		return Snapshot{Regions: make(map[uint64]localmeta.RegionMeta)}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Snapshot{
		Regions:   localmeta.CloneRegionMetas(s.snapshot.Regions),
		Allocator: s.snapshot.Allocator,
	}, nil
}

// SaveRegion publishes the latest descriptor truth for one region.
func (s *RootStore) SaveRegion(meta localmeta.RegionMeta) error {
	if s == nil || meta.ID == 0 {
		return nil
	}
	state, err := s.root.Current()
	if err != nil {
		return err
	}
	desc := descriptor.FromRegionMeta(meta, state.ClusterEpoch+1)
	commit, err := s.root.Append(rootpkg.Event{
		Kind: rootpkg.EventKindRegionDescriptorPublished,
		RegionDescriptor: &rootpkg.RegionDescriptorRecord{
			Descriptor: desc,
		},
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.snapshot.Regions == nil {
		s.snapshot.Regions = make(map[uint64]localmeta.RegionMeta)
	}
	s.snapshot.Regions[meta.ID] = desc.ToRegionMeta()
	s.snapshot.Allocator.IDCurrent = commit.State.IDFence
	s.snapshot.Allocator.TSCurrent = commit.State.TSOFence
	s.mu.Unlock()
	return nil
}

// DeleteRegion tombstones one region from the rooted catalog.
func (s *RootStore) DeleteRegion(regionID uint64) error {
	if s == nil || regionID == 0 {
		return nil
	}
	commit, err := s.root.Append(rootpkg.Event{
		Kind:          rootpkg.EventKindRegionTombstoned,
		RegionRemoval: &rootpkg.RegionRemoval{RegionID: regionID},
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.snapshot.Regions, regionID)
	s.snapshot.Allocator.IDCurrent = commit.State.IDFence
	s.snapshot.Allocator.TSCurrent = commit.State.TSOFence
	s.mu.Unlock()
	return nil
}

// SaveAllocatorState raises allocator fences in the metadata root.
func (s *RootStore) SaveAllocatorState(idCurrent, tsCurrent uint64) error {
	if s == nil {
		return nil
	}
	idFence, err := s.root.FenceAllocator(rootpkg.AllocatorKindID, idCurrent)
	if err != nil {
		return err
	}
	tsoFence, err := s.root.FenceAllocator(rootpkg.AllocatorKindTSO, tsCurrent)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.snapshot.Allocator = AllocatorState{IDCurrent: idFence, TSCurrent: tsoFence}
	s.mu.Unlock()
	return nil
}

// Close releases storage resources.
func (s *RootStore) Close() error { return nil }

func (s *RootStore) reload() error {
	if s == nil || s.root == nil {
		return nil
	}
	state, err := s.root.Current()
	if err != nil {
		return err
	}
	events, _, err := s.root.ReadSince(rootpkg.Cursor{})
	if err != nil {
		return err
	}
	snapshot := Snapshot{
		Regions: make(map[uint64]localmeta.RegionMeta),
		Allocator: AllocatorState{
			IDCurrent: state.IDFence,
			TSCurrent: state.TSOFence,
		},
	}
	for _, evt := range events {
		applyRootEvent(&snapshot, evt)
	}
	s.mu.Lock()
	s.snapshot = snapshot
	s.mu.Unlock()
	return nil
}

func applyRootEvent(snapshot *Snapshot, event rootpkg.Event) {
	if snapshot == nil {
		return
	}
	switch {
	case event.RegionDescriptor != nil:
		snapshot.Regions[event.RegionDescriptor.Descriptor.RegionID] = event.RegionDescriptor.Descriptor.ToRegionMeta()
	case event.RegionRemoval != nil:
		delete(snapshot.Regions, event.RegionRemoval.RegionID)
	case event.RangeSplit != nil:
		delete(snapshot.Regions, event.RangeSplit.ParentRegionID)
		snapshot.Regions[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.ToRegionMeta()
		snapshot.Regions[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.ToRegionMeta()
	case event.RangeMerge != nil:
		delete(snapshot.Regions, event.RangeMerge.LeftRegionID)
		delete(snapshot.Regions, event.RangeMerge.RightRegionID)
		snapshot.Regions[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.ToRegionMeta()
	case event.PeerChange != nil:
		snapshot.Regions[event.PeerChange.Region.RegionID] = event.PeerChange.Region.ToRegionMeta()
	}
}
