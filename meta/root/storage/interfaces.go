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

// CommittedStream is one retained committed rooted stream starting at one
// durable log offset.
type CommittedStream struct {
	Offset    int64
	EndOffset int64
	Records   []CommittedEvent
}

// CloneCommittedStream returns a detached committed-stream view.
func CloneCommittedStream(in CommittedStream) CommittedStream {
	return CommittedStream{
		Offset:    in.Offset,
		EndOffset: in.EndOffset,
		Records:   CloneCommittedEvents(in.Records),
	}
}

// Substrate is the rooted metadata virtual-log surface consumed by root
// backends. It combines one compact checkpoint boundary with one retained
// committed stream and bootstrap installation semantics.
type Substrate interface {
	LoadCheckpoint() (checkpoint Checkpoint, err error)
	SaveCheckpoint(checkpoint Checkpoint) error
	ReadCommitted(offset int64) (CommittedStream, error)
	AppendCommitted(records ...CommittedEvent) (logEnd int64, err error)
	CompactCommitted(stream CommittedStream) error
	InstallBootstrap(checkpoint Checkpoint, stream CommittedStream) error
	Size() (int64, error)
}
