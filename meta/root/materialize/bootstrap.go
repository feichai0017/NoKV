package materialize

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// Bootstrap is one materialized rooted snapshot plus its retained committed tail.
type Bootstrap struct {
	Snapshot   rootstate.Snapshot
	Tail       rootstorage.CommittedTail
	RetainFrom rootstate.Cursor
}

// LoadBootstrap loads one rooted checkpoint and replays retained committed events on top of it.
func LoadBootstrap(storage rootstorage.Substrate) (Bootstrap, error) {
	checkpoint, err := storage.LoadCheckpoint()
	if err != nil {
		return Bootstrap{}, err
	}
	stream, err := storage.ReadCommitted(checkpoint.TailOffset)
	if err != nil {
		return Bootstrap{}, err
	}
	snapshot := checkpoint.Snapshot
	for _, rec := range stream.Records {
		if rootstate.CursorAfter(rec.Cursor, snapshot.State.LastCommitted) {
			rootstate.ApplyEventToState(&snapshot.State, rec.Cursor, rec.Event)
			ApplyEventToDescriptors(snapshot.Descriptors, rec.Event)
		}
	}
	return Bootstrap{
		Snapshot:   snapshot,
		Tail:       stream,
		RetainFrom: stream.RetainFrom(snapshot.State.LastCommitted),
	}, nil
}

// CloneCommittedEvents returns a detached committed-event slice.
func CloneCommittedEvents(in []rootstorage.CommittedEvent) []rootstorage.CommittedEvent {
	if len(in) == 0 {
		return nil
	}
	out := make([]rootstorage.CommittedEvent, 0, len(in))
	for _, rec := range in {
		out = append(out, rootstorage.CommittedEvent{
			Cursor: rec.Cursor,
			Event:  rootevent.CloneEvent(rec.Event),
		})
	}
	return out
}
