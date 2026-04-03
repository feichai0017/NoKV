package local

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

func after(a, b rootstate.Cursor) bool {
	return rootstate.CursorAfter(a, b)
}

func previousCursor(in rootstate.Cursor) rootstate.Cursor {
	if in.Index <= 1 {
		return rootstate.Cursor{}
	}
	return rootstate.Cursor{Term: in.Term, Index: in.Index - 1}
}

func retainedFloor(records []rootstorage.CommittedEvent, fallback rootstate.Cursor) rootstate.Cursor {
	if len(records) == 0 {
		return fallback
	}
	return previousCursor(records[0].Cursor)
}
