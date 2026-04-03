package storage

import (
	"bytes"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootreplicated "github.com/feichai0017/NoKV/meta/root/backend/replicated"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"fmt"
	"slices"
	"sync"
)

// RootStore persists PD truth on top of the metadata root and reconstructs the
// region catalog by replaying committed root events.
type RootStore struct {
	root interface {
		rootpkg.StateReader
		rootpkg.EventAppender
		rootpkg.AllocatorFencer
	}

	mu       sync.RWMutex
	snapshot Snapshot
}

type replicatedRootEntry struct {
	ids     []uint64
	cluster *rootreplicated.FixedCluster
}

var replicatedRootRegistry struct {
	mu       sync.Mutex
	clusters map[string]replicatedRootEntry
}

// OpenRootStore opens a PD storage backend backed by the metadata root.
func OpenRootStore(root interface {
	rootpkg.StateReader
	rootpkg.EventAppender
	rootpkg.AllocatorFencer
}) (*RootStore, error) {
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

// OpenRootReplicatedStore opens one PD storage backend backed by the
// experimental in-process fixed-cluster replicated metadata root.
func OpenRootReplicatedStore(workdir string, nodeID uint64, clusterIDs []uint64) (*RootStore, error) {
	if workdir == "" {
		return nil, fmt.Errorf("pd/storage: workdir is required for replicated root mode")
	}
	if nodeID == 0 {
		return nil, fmt.Errorf("pd/storage: replicated root node id must be > 0")
	}
	cluster, err := getOrCreateReplicatedCluster(workdir, clusterIDs)
	if err != nil {
		return nil, err
	}
	driver, err := cluster.Driver(nodeID)
	if err != nil {
		return nil, err
	}
	root, err := rootreplicated.Open(rootreplicated.Config{Driver: driver})
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
		Descriptors: rootstate.CloneDescriptors(s.snapshot.Descriptors),
		Allocator:   s.snapshot.Allocator,
	}, nil
}

// Refresh reloads the reconstructed PD snapshot from the underlying metadata root.
func (s *RootStore) Refresh() error {
	if s == nil {
		return nil
	}
	if refresher, ok := s.root.(interface{ Refresh() error }); ok {
		if err := refresher.Refresh(); err != nil {
			return err
		}
	}
	return s.reload()
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
	s.mu.RUnlock()

	if existed && descriptorsEqual(prev, desc) {
		return nil
	}

	event := regionEvent(existed, desc)
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

// AppendRootEvent persists one explicit rooted metadata event.
func (s *RootStore) AppendRootEvent(event rootevent.Event) error {
	if s == nil || s.root == nil || event.Kind == rootevent.KindUnknown {
		return nil
	}
	commit, err := s.root.Append(event)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.snapshot.Descriptors == nil {
		s.snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	rootmaterialize.ApplyEventToDescriptors(s.snapshot.Descriptors, event)
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
	commit, err := s.root.Append(rootevent.RegionTombstoned(regionID))
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
		Descriptors: rootstate.CloneDescriptors(snapshot.Descriptors),
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

func regionEvent(existed bool, next descriptor.Descriptor) rootevent.Event {
	if !existed {
		return rootevent.RegionBootstrapped(next)
	}
	return rootevent.RegionDescriptorPublished(next)
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

func getOrCreateReplicatedCluster(workdir string, clusterIDs []uint64) (*rootreplicated.FixedCluster, error) {
	replicatedRootRegistry.mu.Lock()
	defer replicatedRootRegistry.mu.Unlock()
	if replicatedRootRegistry.clusters == nil {
		replicatedRootRegistry.clusters = make(map[string]replicatedRootEntry)
	}
	if entry, ok := replicatedRootRegistry.clusters[workdir]; ok {
		if !slices.Equal(entry.ids, clusterIDs) {
			return nil, fmt.Errorf("pd/storage: replicated root cluster ids for %q changed from %v to %v", workdir, entry.ids, clusterIDs)
		}
		return entry.cluster, nil
	}
	cluster, err := rootreplicated.NewFixedCluster(clusterIDs...)
	if err != nil {
		return nil, err
	}
	replicatedRootRegistry.clusters[workdir] = replicatedRootEntry{
		ids:     slices.Clone(clusterIDs),
		cluster: cluster,
	}
	return cluster, nil
}
