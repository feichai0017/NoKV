package operator

import (
	"fmt"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"sync"
	"time"
)

// Runtime hosts the mutable operator-facing transition runtime inside Coordinator. It
// is rebuilt from rooted transitions while preserving local scheduling state
// such as ownership, admission, and conflict backoff.
type Runtime struct {
	mu      sync.RWMutex
	entries []RuntimeEntry
	state   map[string]RuntimeEntry
	clock   func() time.Time
}

func NewRuntime() *Runtime {
	return &Runtime{
		state: make(map[string]RuntimeEntry),
		clock: time.Now,
	}
}

func (r *Runtime) ReplaceRootedTransitions(entries []rootstate.TransitionEntry) {
	if r == nil {
		return
	}
	now := r.now()
	nextEntries := make([]RuntimeEntry, 0, len(entries))
	nextState := make(map[string]RuntimeEntry, len(entries))

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rooted := range entries {
		key := runtimeKey(rooted)
		entry, ok := r.state[key]
		if !ok {
			entry = RuntimeEntry{
				Owner:   defaultOwner,
				Attempt: 1,
			}
		}
		entry.Transition = cloneTransitionEntry(rooted)
		entry = reconcileEntry(now, entry)
		nextEntries = append(nextEntries, cloneEntry(entry))
		nextState[key] = cloneEntry(entry)
	}
	r.entries = nextEntries
	r.state = nextState
}

func (r *Runtime) Snapshot() RuntimeSnapshot {
	if r == nil {
		return RuntimeSnapshot{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RuntimeEntry, 0, len(r.entries))
	for _, entry := range r.entries {
		out = append(out, cloneEntry(entry))
	}
	return RuntimeSnapshot{Entries: out}
}

func (r *Runtime) now() time.Time {
	if r == nil || r.clock == nil {
		return time.Now()
	}
	return r.clock()
}

func reconcileEntry(now time.Time, entry RuntimeEntry) RuntimeEntry {
	switch entry.Transition.Status {
	case rootstate.TransitionStatusPending, rootstate.TransitionStatusOpen:
		entry.Admitted = true
		entry.BackoffUntil = time.Time{}
	case rootstate.TransitionStatusConflict:
		entry.Admitted = false
		if entry.BackoffUntil.IsZero() || !entry.BackoffUntil.After(now) {
			entry.BackoffUntil = now.Add(defaultConflictBackoff)
		}
	default:
		entry.Admitted = false
		entry.BackoffUntil = time.Time{}
	}
	if entry.Owner == "" {
		entry.Owner = defaultOwner
	}
	if entry.Attempt == 0 {
		entry.Attempt = 1
	}
	return entry
}

func runtimeKey(entry rootstate.TransitionEntry) string {
	if entry.ID != "" {
		return entry.ID
	}
	switch {
	case entry.PeerChange != nil:
		return fmt.Sprintf("peer:%d:%d:%d:%d", entry.Key, entry.PeerChange.Kind, entry.PeerChange.StoreID, entry.PeerChange.PeerID)
	case entry.RangeChange != nil:
		return fmt.Sprintf("range:%d:%d:%d:%d", entry.Key, entry.RangeChange.Kind, entry.RangeChange.LeftRegionID, entry.RangeChange.RightRegionID)
	default:
		return fmt.Sprintf("%d:%d", entry.Kind, entry.Key)
	}
}

func cloneEntry(in RuntimeEntry) RuntimeEntry {
	out := in
	out.Transition = cloneTransitionEntry(in.Transition)
	return out
}

func cloneTransitionEntry(in rootstate.TransitionEntry) rootstate.TransitionEntry {
	out := in
	if in.PeerChange != nil {
		change := *in.PeerChange
		out.PeerChange = &change
	}
	if in.RangeChange != nil {
		change := *in.RangeChange
		out.RangeChange = &change
	}
	return out
}
