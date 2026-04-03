package storage

import (
	"bytes"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/local"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
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
func (s *RootStore) PublishRegionDescriptor(desc descriptor.Descriptor) error {
	if s == nil || desc.RegionID == 0 {
		return nil
	}
	state, err := s.root.Current()
	if err != nil {
		return err
	}
	if desc.RootEpoch == 0 {
		desc.RootEpoch = state.ClusterEpoch + 1
	}

	s.mu.RLock()
	prev, existed := s.snapshot.Descriptors[desc.RegionID]
	current := cloneDescriptors(s.snapshot.Descriptors)
	s.mu.RUnlock()

	if existed {
		if descriptorsEqual(prev, desc) {
			return nil
		}
	}

	event := regionEvent(prev, existed, desc, current)
	commit, err := s.root.Append(event)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.snapshot.Descriptors == nil {
		s.snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	s.snapshot.Descriptors[desc.RegionID] = desc.Clone()
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
func (s *RootStore) Close() error {
	if s == nil {
		return nil
	}
	if closer, ok := s.root.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (s *RootStore) reload() error {
	if s == nil || s.root == nil {
		return nil
	}
	snapshot, err := s.root.Snapshot()
	if err != nil {
		return err
	}
	out := Snapshot{
		Descriptors: cloneDescriptors(snapshot.Descriptors),
		Allocator: AllocatorState{
			IDCurrent: snapshot.State.IDFence,
			TSCurrent: snapshot.State.TSOFence,
		},
	}
	s.mu.Lock()
	s.snapshot = out
	s.mu.Unlock()
	return nil
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

func regionEvent(prev descriptor.Descriptor, existed bool, next descriptor.Descriptor, current map[uint64]descriptor.Descriptor) rootpkg.Event {
	if split, ok := classifySplitEventFromLineage(next, current); ok {
		return split
	}
	if merge, ok := classifyMergeEventFromLineage(prev, existed, next, current); ok {
		return merge
	}
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

func classifySplitEventFromLineage(next descriptor.Descriptor, current map[uint64]descriptor.Descriptor) (rootpkg.Event, bool) {
	for _, ref := range next.Lineage {
		if ref.Kind != descriptor.LineageKindSplitParent || ref.RegionID == 0 || ref.RegionID == next.RegionID {
			continue
		}
		parent, ok := current[ref.RegionID]
		if !ok {
			return rootpkg.Event{}, false
		}
		return rootpkg.RegionSplitCommitted(ref.RegionID, next.StartKey, parent.Clone(), next), true
	}
	return rootpkg.Event{}, false
}

func classifyMergeEventFromLineage(prev descriptor.Descriptor, existed bool, next descriptor.Descriptor, current map[uint64]descriptor.Descriptor) (rootpkg.Event, bool) {
	if !existed {
		return rootpkg.Event{}, false
	}
	for _, ref := range next.Lineage {
		if ref.Kind != descriptor.LineageKindMergeSource || ref.RegionID == 0 || ref.RegionID == next.RegionID {
			continue
		}
		source, ok := current[ref.RegionID]
		if !ok {
			return rootpkg.Event{}, false
		}
		leftID, rightID := next.RegionID, source.RegionID
		if bytes.Compare(source.StartKey, next.StartKey) < 0 {
			leftID, rightID = source.RegionID, next.RegionID
		}
		return rootpkg.RegionMerged(leftID, rightID, next), true
	}
	return rootpkg.Event{}, false
}

func peerDelta(prev, next []metaregion.Peer) (added, removed []metaregion.Peer) {
	prevSet := make(map[uint64]metaregion.Peer, len(prev))
	nextSet := make(map[uint64]metaregion.Peer, len(next))
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
