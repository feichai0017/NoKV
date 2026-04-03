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

// EventWriter appends ordered rooted metadata events.
type EventWriter interface {
	Append(events ...rootevent.Event) (rootstate.CommitInfo, error)
}

// AllocatorFenceWriter advances global allocator fences monotonically.
type AllocatorFenceWriter interface {
	FenceAllocator(kind AllocatorKind, min uint64) (uint64, error)
}
