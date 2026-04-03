package root

// StateReader exposes compact rooted metadata state.
type StateReader interface {
	Current() (State, error)
	Snapshot() (Snapshot, error)
}

// EventReader exposes retained rooted event history.
type EventReader interface {
	// ReadSince returns retained root events after cursor. Implementations may
	// compact older history, so callers should use Snapshot for bounded
	// bootstrap/recovery rather than assuming a full event log from zero.
	ReadSince(cursor Cursor) ([]Event, Cursor, error)
}

// EventAppender appends ordered rooted metadata events.
type EventAppender interface {
	Append(events ...Event) (CommitInfo, error)
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
