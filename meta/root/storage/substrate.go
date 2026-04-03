package storage

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// Checkpoint is one compact rooted snapshot plus the retained-log offset to
// continue bootstrap replay from.
type Checkpoint struct {
	Snapshot   rootstate.Snapshot
	TailOffset int64
}

// CloneCheckpoint returns a detached rooted checkpoint.
func CloneCheckpoint(in Checkpoint) Checkpoint {
	return Checkpoint{
		Snapshot:   rootstate.CloneSnapshot(in.Snapshot),
		TailOffset: in.TailOffset,
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

// CommittedTail is one retained committed rooted stream starting at one
// durable log offset.
type CommittedTail struct {
	Offset    int64
	EndOffset int64
	Records   []CommittedEvent
}

// CloneCommittedTail returns a detached committed-stream view.
func CloneCommittedTail(in CommittedTail) CommittedTail {
	return CommittedTail{
		Offset:    in.Offset,
		EndOffset: in.EndOffset,
		Records:   CloneCommittedEvents(in.Records),
	}
}

// RetainFrom returns the cursor immediately before the first retained event.
// When the stream is empty, fallback is returned unchanged.
func (s CommittedTail) RetainFrom(fallback rootstate.Cursor) rootstate.Cursor {
	if len(s.Records) == 0 {
		return fallback
	}
	first := s.Records[0].Cursor
	if first.Index <= 1 {
		return rootstate.Cursor{}
	}
	return rootstate.Cursor{Term: first.Term, Index: first.Index - 1}
}

// TailCursor returns the last committed cursor visible in this retained
// stream. When the stream is empty, fallback is returned unchanged.
func (s CommittedTail) TailCursor(fallback rootstate.Cursor) rootstate.Cursor {
	if len(s.Records) == 0 {
		return fallback
	}
	return s.Records[len(s.Records)-1].Cursor
}

// Substrate is the rooted metadata virtual-log surface consumed by root
// backends. It combines one compact checkpoint boundary with one retained
// committed stream and bootstrap installation semantics.
type Substrate interface {
	LoadCheckpoint() (checkpoint Checkpoint, err error)
	SaveCheckpoint(checkpoint Checkpoint) error
	ReadCommitted(offset int64) (CommittedTail, error)
	AppendCommitted(records ...CommittedEvent) (logEnd int64, err error)
	CompactCommitted(stream CommittedTail) error
	InstallBootstrap(checkpoint Checkpoint, stream CommittedTail) error
	Size() (int64, error)
}
