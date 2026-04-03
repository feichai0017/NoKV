package storage

import rootpkg "github.com/feichai0017/NoKV/meta/root"

// CommittedEvent is one rooted metadata event paired with its committed cursor.
type CommittedEvent struct {
	Cursor rootpkg.Cursor
	Event  rootpkg.Event
}

// EventLog exposes the ordered committed metadata log surface needed by root
// implementations.
type EventLog interface {
	Load(offset int64) ([]CommittedEvent, error)
	Append(records ...CommittedEvent) (logEnd int64, err error)
	Rewrite(records []CommittedEvent) error
	Size() (int64, error)
}

// CheckpointStore persists compact rooted metadata snapshots and their
// associated retained-log boundary.
type CheckpointStore interface {
	Load() (snapshot rootpkg.Snapshot, logOffset int64, err error)
	Save(snapshot rootpkg.Snapshot, logOffset uint64) error
}
