package materialize

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// Bootstrap is one materialized rooted snapshot plus its retained committed tail.
type Bootstrap struct {
	Snapshot   rootstate.Snapshot
	Records    []rootstorage.CommittedEvent
	LogOffset  int64
	RetainFrom rootstate.Cursor
}

// LoadBootstrap loads one rooted checkpoint and replays retained committed events on top of it.
func LoadBootstrap(checkpt rootstorage.CheckpointStore, log rootstorage.EventLog) (Bootstrap, error) {
	checkpoint, err := checkpt.Load()
	if err != nil {
		return Bootstrap{}, err
	}
	records, err := log.Load(checkpoint.LogOffset)
	if err != nil {
		return Bootstrap{}, err
	}
	snapshot := checkpoint.Snapshot
	for _, rec := range records {
		if rootstate.CursorAfter(rec.Cursor, snapshot.State.LastCommitted) {
			rootstate.ApplyEventToState(&snapshot.State, rec.Cursor, rec.Event)
			ApplyEventToDescriptors(snapshot.Descriptors, rec.Event)
		}
	}
	return Bootstrap{
		Snapshot:   snapshot,
		Records:    records,
		LogOffset:  checkpoint.LogOffset,
		RetainFrom: RetainedFloor(records, snapshot.State.LastCommitted),
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

// RetainedFloor returns the cursor immediately before the first retained event.
func RetainedFloor(records []rootstorage.CommittedEvent, fallback rootstate.Cursor) rootstate.Cursor {
	if len(records) == 0 {
		return fallback
	}
	return previousCursor(records[0].Cursor)
}

func previousCursor(in rootstate.Cursor) rootstate.Cursor {
	if in.Index <= 1 {
		return rootstate.Cursor{}
	}
	return rootstate.Cursor{Term: in.Term, Index: in.Index - 1}
}
