package root

import rootstate "github.com/feichai0017/NoKV/meta/root/state"

// ApplyEventToState applies one rooted metadata event into compact root state.
func ApplyEventToState(state *State, cursor Cursor, event Event) {
	rootstate.ApplyEventToState(state, cursor, event)
}

// NextCursor returns the next ordered root cursor.
func NextCursor(prev Cursor) Cursor { return rootstate.NextCursor(prev) }

// CursorAfter reports whether a is ordered strictly after b.
func CursorAfter(a, b Cursor) bool { return rootstate.CursorAfter(a, b) }
