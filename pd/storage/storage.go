package storage

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"math"
	"slices"
)

// AllocatorState captures persisted counters for ID and TSO allocators.
type AllocatorState struct {
	IDCurrent uint64
	TSCurrent uint64
}

// Snapshot is the reconstructed PD bootstrap catalog derived from durable
// metadata-root truth.
type Snapshot struct {
	ClusterEpoch       uint64
	Descriptors        map[uint64]descriptor.Descriptor
	PendingPeerChanges map[uint64]rootstate.PendingPeerChange
	Allocator          AllocatorState
}

// BootstrapInfo captures rooted PD bootstrap results.
type BootstrapInfo struct {
	LoadedRegions int
	IDStart       uint64
	TSStart       uint64
	Snapshot      Snapshot
}

// Store persists control-plane mutations into durable metadata truth and
// exposes the reconstructed rooted snapshot back to PD.
type Store interface {
	// Load returns the reconstructed snapshot.
	Load() (Snapshot, error)
	// AppendRootEvent persists one explicit rooted truth event.
	AppendRootEvent(event rootevent.Event) error
	// SaveAllocatorState persists latest allocator counters.
	SaveAllocatorState(idCurrent, tsCurrent uint64) error
	// Refresh reloads the reconstructed rooted snapshot from the underlying root.
	Refresh() error
	// IsLeader reports whether the current rooted storage instance is writable.
	IsLeader() bool
	// LeaderID reports the current rooted leader when known.
	LeaderID() uint64
	// Close releases storage resources.
	Close() error
}

// NoopStore is an in-memory/no-op storage implementation.
type NoopStore struct{}

// NewNoopStore creates a no-op PD storage.
func NewNoopStore() Store {
	return NoopStore{}
}

// Load returns an empty snapshot.
func (NoopStore) Load() (Snapshot, error) {
	return Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
}

// AppendRootEvent is a no-op.
func (NoopStore) AppendRootEvent(rootevent.Event) error {
	return nil
}

// SaveAllocatorState is a no-op.
func (NoopStore) SaveAllocatorState(uint64, uint64) error {
	return nil
}

// Refresh is a no-op.
func (NoopStore) Refresh() error {
	return nil
}

// IsLeader always reports writable in no-op mode.
func (NoopStore) IsLeader() bool {
	return true
}

// LeaderID reports no separate leader in no-op mode.
func (NoopStore) LeaderID() uint64 {
	return 0
}

// Close is a no-op.
func (NoopStore) Close() error {
	return nil
}

// ResolveAllocatorStarts raises starts to checkpoint+1 when needed.
func ResolveAllocatorStarts(idStart, tsStart uint64, state AllocatorState) (uint64, uint64) {
	nextID := state.IDCurrent
	if nextID < math.MaxUint64 {
		nextID++
	}
	if nextID > idStart {
		idStart = nextID
	}

	nextTS := state.TSCurrent
	if nextTS < math.MaxUint64 {
		nextTS++
	}
	if nextTS > tsStart {
		tsStart = nextTS
	}
	return idStart, tsStart
}

// RestoreDescriptors replays a rooted descriptor catalog into one runtime cluster view.
func RestoreDescriptors(apply func(descriptor.Descriptor) error, descriptors map[uint64]descriptor.Descriptor) (int, error) {
	if apply == nil || len(descriptors) == 0 {
		return 0, nil
	}
	ids := make([]uint64, 0, len(descriptors))
	for id := range descriptors {
		if id == 0 {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)

	loaded := 0
	for _, id := range ids {
		desc := descriptors[id]
		if desc.RegionID == 0 {
			continue
		}
		if err := apply(desc); err != nil {
			return loaded, err
		}
		loaded++
	}
	return loaded, nil
}

// Bootstrap reconstructs one PD runtime view from rooted durable metadata and
// resolves allocator starts against persisted fences.
func Bootstrap(store Store, apply func(descriptor.Descriptor) error, idStart, tsStart uint64) (BootstrapInfo, error) {
	if store == nil {
		return BootstrapInfo{IDStart: idStart, TSStart: tsStart}, nil
	}
	snapshot, err := store.Load()
	if err != nil {
		return BootstrapInfo{}, err
	}
	loadedRegions, err := RestoreDescriptors(apply, snapshot.Descriptors)
	if err != nil {
		return BootstrapInfo{}, err
	}
	idStart, tsStart = ResolveAllocatorStarts(idStart, tsStart, snapshot.Allocator)
	return BootstrapInfo{
		LoadedRegions: loadedRegions,
		IDStart:       idStart,
		TSStart:       tsStart,
		Snapshot:      snapshot,
	}, nil
}
