package operator

import rootstate "github.com/feichai0017/NoKV/meta/root/state"

// Entry is one rooted transition surfaced through the PD operator runtime.
type Entry = rootstate.TransitionEntry

// Snapshot captures the operator-runtime view derived from rooted transitions.
type Snapshot struct {
	Entries []Entry
}

