package root

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// SnapshotReader exposes compact rooted metadata state.
type SnapshotReader interface {
	Current() (rootstate.State, error)
	Snapshot() (rootstate.Snapshot, error)
}

// HistoryReader exposes retained rooted event history.
type HistoryReader interface {
	// ReadSince returns retained root events after cursor. Implementations may
	// compact older history, so callers should use Snapshot for bounded
	// bootstrap/recovery rather than assuming a full event log from zero.
	ReadSince(cursor rootstate.Cursor) ([]rootevent.Event, rootstate.Cursor, error)
}

// EventWriter appends ordered rooted metadata events.
type EventWriter interface {
	Append(events ...rootevent.Event) (rootstate.CommitInfo, error)
}

// AllocatorFenceWriter advances global allocator fences monotonically.
type AllocatorFenceWriter interface {
	FenceAllocator(kind AllocatorKind, min uint64) (uint64, error)
}

// Backend is the full metadata-root interface exposed by current implementations.
//
// The implementation may be local or replicated. Callers should depend on the
// narrowest interface they actually need.
type Backend interface {
	SnapshotReader
	HistoryReader
	EventWriter
	AllocatorFenceWriter
}
