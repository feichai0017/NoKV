package root

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// StateReader exposes compact rooted metadata state.
type StateReader interface {
	Current() (rootstate.State, error)
	Snapshot() (rootstate.Snapshot, error)
}

// EventReader exposes retained rooted event history.
type EventReader interface {
	// ReadSince returns retained root events after cursor. Implementations may
	// compact older history, so callers should use Snapshot for bounded
	// bootstrap/recovery rather than assuming a full event log from zero.
	ReadSince(cursor rootstate.Cursor) ([]rootevent.Event, rootstate.Cursor, error)
}

// EventAppender appends ordered rooted metadata events.
type EventAppender interface {
	Append(events ...rootevent.Event) (rootstate.CommitInfo, error)
}

// AllocatorFencer advances global allocator fences monotonically.
type AllocatorFencer interface {
	FenceAllocator(kind AllocatorKind, min uint64) (uint64, error)
}

// Root is the full metadata-root interface exposed by current implementations.
//
// The implementation may be local, replicated, or mock-backed. Callers should
// depend on the narrowest capability interface they actually need.
type Root interface {
	StateReader
	EventReader
	EventAppender
	AllocatorFencer
}
