package storage

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// Checkpoint is one compact rooted snapshot plus the retained-log offset to
// continue bootstrap replay from.
type Checkpoint struct {
	Snapshot  rootstate.Snapshot
	LogOffset int64
}

// CloneCheckpoint returns a detached rooted checkpoint.
func CloneCheckpoint(in Checkpoint) Checkpoint {
	return Checkpoint{
		Snapshot:  rootstate.CloneSnapshot(in.Snapshot),
		LogOffset: in.LogOffset,
	}
}

// CommittedEvent is one rooted metadata event paired with its committed cursor.
type CommittedEvent struct {
	Cursor rootstate.Cursor
	Event  rootevent.Event
}

// CloneCommittedEvents returns a detached committed-event slice.
func CloneCommittedEvents(in []CommittedEvent) []CommittedEvent {
	if len(in) == 0 {
		return nil
	}
	out := make([]CommittedEvent, 0, len(in))
	for _, rec := range in {
		out = append(out, CommittedEvent{
			Cursor: rec.Cursor,
			Event:  rootevent.CloneEvent(rec.Event),
		})
	}
	return out
}

// EventLog exposes the ordered committed metadata log surface needed by root implementations.
type EventLog interface {
	Load(offset int64) ([]CommittedEvent, error)
	Append(records ...CommittedEvent) (logEnd int64, err error)
	Compact(records []CommittedEvent) error
	Size() (int64, error)
}

// CheckpointStore persists compact rooted metadata snapshots and their associated retained-log boundary.
type CheckpointStore interface {
	Load() (checkpoint Checkpoint, err error)
	Save(checkpoint Checkpoint) error
}

// BootstrapInstaller installs one checkpoint and retained committed tail as a
// new root bootstrap boundary. Implementations should replace prior retained
// state atomically when possible.
type BootstrapInstaller interface {
	InstallBootstrap(checkpoint Checkpoint, records []CommittedEvent) error
}
