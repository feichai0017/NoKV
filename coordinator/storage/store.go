package storage

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// Store persists control-plane mutations into durable metadata truth and
// exposes the reconstructed rooted snapshot back to Coordinator.
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

// noopStore is an in-memory/no-op storage implementation used in package-local
// tests and empty fallback paths.
type noopStore struct{}

// Load returns an empty snapshot.
func (noopStore) Load() (Snapshot, error) {
	return Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
}

// AppendRootEvent is a no-op.
func (noopStore) AppendRootEvent(rootevent.Event) error {
	return nil
}

// SaveAllocatorState is a no-op.
func (noopStore) SaveAllocatorState(uint64, uint64) error {
	return nil
}

// Refresh is a no-op.
func (noopStore) Refresh() error {
	return nil
}

// IsLeader always reports writable in no-op mode.
func (noopStore) IsLeader() bool {
	return true
}

// LeaderID reports no separate leader in no-op mode.
func (noopStore) LeaderID() uint64 {
	return 0
}

// Close is a no-op.
func (noopStore) Close() error {
	return nil
}
