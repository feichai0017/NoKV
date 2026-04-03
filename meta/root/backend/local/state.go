package local

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

func after(a, b rootpkg.Cursor) bool {
	return rootpkg.CursorAfter(a, b)
}

func previousCursor(in rootpkg.Cursor) rootpkg.Cursor {
	if in.Index <= 1 {
		return rootpkg.Cursor{}
	}
	return rootpkg.Cursor{Term: in.Term, Index: in.Index - 1}
}

func retainedFloor(records []rootstorage.CommittedEvent, fallback rootpkg.Cursor) rootpkg.Cursor {
	if len(records) == 0 {
		return fallback
	}
	return previousCursor(records[0].Cursor)
}
