package storage

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"math"
	"slices"
	"time"
)

// AllocatorState captures persisted counters for ID and TSO allocators.
type AllocatorState struct {
	IDCurrent uint64
	TSCurrent uint64
}

// Snapshot is the reconstructed PD bootstrap catalog derived from durable
// metadata-root truth.
type Snapshot struct {
	Descriptors map[uint64]descriptor.Descriptor
	Allocator   AllocatorState
}

// BootstrapInfo captures rooted PD bootstrap results.
type BootstrapInfo struct {
	LoadedRegions int
	IDStart       uint64
	TSStart       uint64
	Snapshot      Snapshot
}

// Loader reconstructs a bootstrap snapshot from durable metadata truth.
type Loader interface {
	// Load returns the reconstructed snapshot.
	Load() (Snapshot, error)
	// Close releases storage resources.
	Close() error
}

// Sink persists control-plane mutations into durable metadata truth.
type Sink interface {
	// PublishRegionDescriptor persists one rooted region descriptor update.
	PublishRegionDescriptor(desc descriptor.Descriptor) error
	// AppendRootEvent persists one explicit rooted truth event.
	AppendRootEvent(event rootevent.Event) error
	// TombstoneRegion persists one rooted region removal.
	TombstoneRegion(regionID uint64) error
	// SaveAllocatorState persists latest allocator counters.
	SaveAllocatorState(idCurrent, tsCurrent uint64) error
	// Close releases storage resources.
	Close() error
}

// Store defines rooted PD persistence behavior.
type Store interface {
	Loader
	Sink
}

// Refresher reloads the reconstructed PD snapshot from the underlying root.
type Refresher interface {
	Refresh() error
}

// ChangeWaiter waits until rooted truth advances past one committed cursor.
type ChangeWaiter interface {
	WaitForChange(after rootstate.Cursor, timeout time.Duration) (rootstate.Cursor, error)
}

// LeaderStatus reports whether the current rooted storage instance is the
// writable leader for metadata truth, and which leader is currently known.
type LeaderStatus interface {
	IsLeader() bool
	LeaderID() uint64
}

// DescriptorCatalog accepts region descriptor updates during PD bootstrap.
type DescriptorCatalog interface {
	PublishRegionDescriptor(desc descriptor.Descriptor) error
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

// PublishRegionDescriptor is a no-op.
func (NoopStore) PublishRegionDescriptor(descriptor.Descriptor) error {
	return nil
}

// AppendRootEvent is a no-op.
func (NoopStore) AppendRootEvent(rootevent.Event) error {
	return nil
}

// TombstoneRegion is a no-op.
func (NoopStore) TombstoneRegion(uint64) error {
	return nil
}

// SaveAllocatorState is a no-op.
func (NoopStore) SaveAllocatorState(uint64, uint64) error {
	return nil
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
func RestoreDescriptors(catalog DescriptorCatalog, descriptors map[uint64]descriptor.Descriptor) (int, error) {
	if catalog == nil || len(descriptors) == 0 {
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
		if err := catalog.PublishRegionDescriptor(desc); err != nil {
			return loaded, err
		}
		loaded++
	}
	return loaded, nil
}

// Bootstrap reconstructs one PD runtime view from rooted durable metadata and
// resolves allocator starts against persisted fences.
func Bootstrap(loader Loader, catalog DescriptorCatalog, idStart, tsStart uint64) (BootstrapInfo, error) {
	if loader == nil {
		return BootstrapInfo{IDStart: idStart, TSStart: tsStart}, nil
	}
	snapshot, err := loader.Load()
	if err != nil {
		return BootstrapInfo{}, err
	}
	loadedRegions, err := RestoreDescriptors(catalog, snapshot.Descriptors)
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
