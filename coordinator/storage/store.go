package storage

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// RootStorage persists control-plane mutations into durable metadata truth and
// exposes the reconstructed rooted snapshot back to Coordinator.
type RootStorage interface {
	// Load returns the reconstructed snapshot.
	Load() (Snapshot, error)
	// AppendRootEvent persists one explicit rooted truth event.
	AppendRootEvent(event rootevent.Event) error
	// SaveAllocatorState persists allocator fences. A fence may be ahead of the
	// latest value served when Coordinator uses a preallocated window.
	SaveAllocatorState(idCurrent, tsCurrent uint64) error
	// ApplyCoordinatorLease applies one rooted coordinator lease command.
	ApplyCoordinatorLease(cmd rootstate.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error)
	// ApplyCoordinatorClosure applies one rooted coordinator closure command.
	ApplyCoordinatorClosure(cmd rootstate.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error)
	// Refresh reloads the reconstructed rooted snapshot from the underlying root.
	Refresh() error
	// IsLeader reports whether the current rooted storage instance is writable.
	IsLeader() bool
	// LeaderID reports the current rooted leader when known.
	LeaderID() uint64
	// Close releases storage resources.
	Close() error
}
