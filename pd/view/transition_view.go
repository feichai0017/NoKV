package view

import (
	"sync"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// TransitionSnapshot captures rooted pending transition state materialized into
// PD runtime view.
type TransitionSnapshot struct {
	PendingPeerChanges  map[uint64]rootstate.PendingPeerChange
	PendingRangeChanges map[uint64]rootstate.PendingRangeChange
}

// TransitionAssessment is one explicit lifecycle assessment for a proposed
// rooted transition event against the current rooted snapshot.
type TransitionAssessment = rootstate.TransitionAssessment

// TransitionView is the disposable runtime view of rooted pending execution
// state tracked by PD.
type TransitionView struct {
	mu                 sync.RWMutex
	pendingPeerChanges map[uint64]rootstate.PendingPeerChange
	pendingRangeChange map[uint64]rootstate.PendingRangeChange
}

func NewTransitionView() *TransitionView {
	return &TransitionView{
		pendingPeerChanges: make(map[uint64]rootstate.PendingPeerChange),
		pendingRangeChange: make(map[uint64]rootstate.PendingRangeChange),
	}
}

func (v *TransitionView) Replace(peers map[uint64]rootstate.PendingPeerChange, ranges map[uint64]rootstate.PendingRangeChange) {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.pendingPeerChanges = rootstate.ClonePendingPeerChanges(peers)
	v.pendingRangeChange = rootstate.ClonePendingRangeChanges(ranges)
	v.mu.Unlock()
}

func (v *TransitionView) Snapshot() TransitionSnapshot {
	if v == nil {
		return TransitionSnapshot{
			PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
			PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
		}
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return TransitionSnapshot{
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(v.pendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(v.pendingRangeChange),
	}
}
