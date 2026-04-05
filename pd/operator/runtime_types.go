package operator

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"time"
)

const (
	defaultOwner           = "pd"
	defaultConflictBackoff = 3 * time.Second
)

// RuntimeEntry is one operator-runtime record derived from rooted transition
// truth. It adds runtime-only scheduling metadata on top of the rooted
// transition assessment without mutating rooted truth.
type RuntimeEntry struct {
	Transition   rootstate.TransitionEntry
	Owner        string
	Attempt      uint64
	Admitted     bool
	BackoffUntil time.Time
}

// RuntimeSnapshot captures the current operator-runtime view inside PD.
type RuntimeSnapshot struct {
	Entries []RuntimeEntry
}
