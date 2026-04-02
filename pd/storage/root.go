package storage

import (
	"bytes"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/local"
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

// OpenRootLocalStore opens a PD storage backend backed by the local metadata
// root files in workdir.
func OpenRootLocalStore(workdir string) (*RootStore, error) {
	root, err := rootlocal.Open(workdir, nil)
	if err != nil {
		return nil, err
	}
	return OpenRootStore(root)
}

// Load returns the last reconstructed snapshot.
func (s *RootStore) Load() (Snapshot, error) {
	if s == nil {
		return Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Snapshot{
		Descriptors: cloneDescriptors(s.snapshot.Descriptors),
		Allocator:   s.snapshot.Allocator,
	}, nil
}

// PublishRegionDescriptor publishes the latest descriptor truth for one region.
func (s *RootStore) PublishRegionDescriptor(meta localmeta.RegionMeta) error {
	if s == nil || meta.ID == 0 {
		return nil
	}
	state, err := s.root.Current()
	if err != nil {
		return err
	}
	desc := descriptor.FromRegionMeta(meta, state.ClusterEpoch+1)

	s.mu.RLock()
	prev, existed := s.snapshot.Descriptors[meta.ID]
	s.mu.RUnlock()

	if existed {
		if descriptorsEqual(prev, desc) {
			return nil
		}
	}

	event := regionEvent(prev, existed, desc)
	commit, err := s.root.Append(event)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.snapshot.Descriptors == nil {
		s.snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	s.snapshot.Descriptors[meta.ID] = desc.Clone()
	s.snapshot.Allocator.IDCurrent = commit.State.IDFence
	s.snapshot.Allocator.TSCurrent = commit.State.TSOFence
	s.mu.Unlock()
	return nil
}

// TombstoneRegion tombstones one region from the rooted catalog.
func (s *RootStore) TombstoneRegion(regionID uint64) error {
	if s == nil || regionID == 0 {
		return nil
	}
	commit, err := s.root.Append(rootpkg.RegionTombstoned(regionID))
	if err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.snapshot.Descriptors, regionID)
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
		Descriptors: make(map[uint64]descriptor.Descriptor),
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
		snapshot.Descriptors[event.RegionDescriptor.Descriptor.RegionID] = event.RegionDescriptor.Descriptor.Clone()
	case event.RegionRemoval != nil:
		delete(snapshot.Descriptors, event.RegionRemoval.RegionID)
	case event.RangeSplit != nil:
		delete(snapshot.Descriptors, event.RangeSplit.ParentRegionID)
		snapshot.Descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
		snapshot.Descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
	case event.RangeMerge != nil:
		delete(snapshot.Descriptors, event.RangeMerge.LeftRegionID)
		delete(snapshot.Descriptors, event.RangeMerge.RightRegionID)
		snapshot.Descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
	case event.PeerChange != nil:
		snapshot.Descriptors[event.PeerChange.Region.RegionID] = event.PeerChange.Region.Clone()
	}
}

func cloneDescriptors(in map[uint64]descriptor.Descriptor) map[uint64]descriptor.Descriptor {
	if len(in) == 0 {
		return make(map[uint64]descriptor.Descriptor)
	}
	out := make(map[uint64]descriptor.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

func regionEvent(prev descriptor.Descriptor, existed bool, next descriptor.Descriptor) rootpkg.Event {
	if !existed {
		return rootpkg.RegionBootstrapped(next)
	}
	added, removed := peerDelta(prev.Peers, next.Peers)
	switch {
	case len(added) == 1 && len(removed) == 0:
		return rootpkg.PeerAdded(next.RegionID, added[0].StoreID, added[0].PeerID, next)
	case len(added) == 0 && len(removed) == 1:
		return rootpkg.PeerRemoved(next.RegionID, removed[0].StoreID, removed[0].PeerID, next)
	default:
		return rootpkg.RegionDescriptorPublished(next)
	}
}

func peerDelta(prev, next []localmeta.PeerMeta) (added, removed []localmeta.PeerMeta) {
	prevSet := make(map[uint64]localmeta.PeerMeta, len(prev))
	nextSet := make(map[uint64]localmeta.PeerMeta, len(next))
	for _, peer := range prev {
		prevSet[peer.PeerID] = peer
	}
	for _, peer := range next {
		nextSet[peer.PeerID] = peer
	}
	for id, peer := range nextSet {
		if _, ok := prevSet[id]; !ok {
			added = append(added, peer)
		}
	}
	for id, peer := range prevSet {
		if _, ok := nextSet[id]; !ok {
			removed = append(removed, peer)
		}
	}
	return added, removed
}

func descriptorsEqual(a, b descriptor.Descriptor) bool {
	if a.RegionID != b.RegionID ||
		a.State != b.State ||
		a.Epoch != b.Epoch ||
		!bytes.Equal(a.StartKey, b.StartKey) ||
		!bytes.Equal(a.EndKey, b.EndKey) ||
		!bytes.Equal(a.Hash, b.Hash) {
		return false
	}
	if len(a.Peers) != len(b.Peers) || len(a.Lineage) != len(b.Lineage) {
		return false
	}
	for i := range a.Peers {
		if a.Peers[i] != b.Peers[i] {
			return false
		}
	}
	for i := range a.Lineage {
		if a.Lineage[i].RegionID != b.Lineage[i].RegionID ||
			a.Lineage[i].Epoch != b.Lineage[i].Epoch ||
			a.Lineage[i].Kind != b.Lineage[i].Kind ||
			!bytes.Equal(a.Lineage[i].Hash, b.Lineage[i].Hash) {
			return false
		}
	}
	return true
}
