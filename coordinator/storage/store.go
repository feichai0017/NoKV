package storage

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

// RootStorage persists control-plane mutations into durable metadata truth and
// exposes the reconstructed rooted snapshot back to Coordinator.
type RootStorage interface {
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
