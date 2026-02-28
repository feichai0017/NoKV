package storage

import (
	"math"

	"github.com/feichai0017/NoKV/manifest"
)

// StateFileName is the allocator checkpoint file name used by local storage.
const StateFileName = "PD_STATE.json"

// AllocatorState captures persisted counters for ID and TSO allocators.
type AllocatorState struct {
	IDCurrent uint64 `json:"id_current"`
	TSCurrent uint64 `json:"ts_current"`
}

// Snapshot contains persisted PD metadata loaded at startup.
type Snapshot struct {
	Regions   map[uint64]manifest.RegionMeta
	Allocator AllocatorState
}

// Store defines PD persistence behavior.
type Store interface {
	// Load returns the persisted snapshot.
	Load() (Snapshot, error)
	// SaveRegion persists one region metadata update.
	SaveRegion(meta manifest.RegionMeta) error
	// DeleteRegion persists one region metadata delete.
	DeleteRegion(regionID uint64) error
	// SaveAllocatorState persists latest allocator counters.
	SaveAllocatorState(idCurrent, tsCurrent uint64) error
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
	return Snapshot{Regions: make(map[uint64]manifest.RegionMeta)}, nil
}

// SaveRegion is a no-op.
func (NoopStore) SaveRegion(manifest.RegionMeta) error {
	return nil
}

// DeleteRegion is a no-op.
func (NoopStore) DeleteRegion(uint64) error {
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
