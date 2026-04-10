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

// BootstrapFromObserved materializes one rooted bootstrap image from one
// observed checkpoint plus retained committed tail.
func BootstrapFromObserved(observed rootstorage.ObservedCommitted) Bootstrap {
	snapshot := observed.Checkpoint.Snapshot
	for _, rec := range observed.Tail.Records {
		if rootstate.CursorAfter(rec.Cursor, snapshot.State.LastCommitted) {
			rootstate.ApplyEventToSnapshot(&snapshot, rec.Cursor, rec.Event)
		}
	}
	return Bootstrap{
		Snapshot:   snapshot,
		Tail:       observed.Tail,
		RetainFrom: observed.RetainFrom(),
	}
}

// LoadBootstrap loads one rooted checkpoint and replays retained committed events on top of it.
func LoadBootstrap(log rootstorage.VirtualLog) (Bootstrap, error) {
	observed, err := rootstorage.ObserveCommitted(log, 0)
	if err != nil {
		return Bootstrap{}, err
	}
	return BootstrapFromObserved(observed), nil
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
