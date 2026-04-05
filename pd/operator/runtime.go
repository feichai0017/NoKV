package operator

import (
	"sync"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// Runtime hosts the mutable operator-facing transition view inside PD. It is
// derived from rooted truth, but intentionally kept out of rooted state.
type Runtime struct {
	mu      sync.RWMutex
	entries []Entry
}

func NewRuntime() *Runtime {
	return &Runtime{}
}

func (r *Runtime) ReplaceRootedTransitions(entries []Entry) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.entries = rootstate.CloneTransitionEntries(entries)
	r.mu.Unlock()
}

func (r *Runtime) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return Snapshot{Entries: rootstate.CloneTransitionEntries(r.entries)}
}

