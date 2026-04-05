package operator

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"time"
)

const (
	defaultOwner           = "pd"
	defaultConflictBackoff = 3 * time.Second
)

// Entry is one operator-runtime record derived from rooted transition truth.
// It adds runtime-only scheduling metadata on top of the rooted transition
// assessment without mutating rooted truth.
type Entry struct {
	Transition   rootstate.TransitionEntry
	Owner        string
	Attempt      uint64
	Admitted     bool
	BackoffUntil time.Time
}

// Snapshot captures the current operator-runtime view inside PD.
type Snapshot struct {
	Entries []Entry
}
