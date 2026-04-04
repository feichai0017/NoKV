package state

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

type PeerChangeLifecycleDecision uint8

const (
	PeerChangeLifecycleApply PeerChangeLifecycleDecision = iota
	PeerChangeLifecycleSkip
)

type PeerChangeCompletionState uint8

const (
	PeerChangeCompletionOpen PeerChangeCompletionState = iota
	PeerChangeCompletionPending
	PeerChangeCompletionCompleted
)

type PeerChangeCompletion struct {
	RegionID uint64
	State    PeerChangeCompletionState
	Pending  PendingPeerChange
}

type PeerChangeLifecycle struct {
	Decision   PeerChangeLifecycleDecision
	Completion PeerChangeCompletion
	Conflict   bool
}

func PendingPeerChangeFromEvent(event rootevent.Event) (PendingPeerChange, bool) {
	if event.PeerChange == nil {
		return PendingPeerChange{}, false
	}
	var kind PendingPeerChangeKind
	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerAdded:
		kind = PendingPeerChangeAddition
	case rootevent.KindPeerRemovalPlanned, rootevent.KindPeerRemoved:
		kind = PendingPeerChangeRemoval
	default:
		return PendingPeerChange{}, false
	}
	return PendingPeerChange{
		Kind:    kind,
		StoreID: event.PeerChange.StoreID,
		PeerID:  event.PeerChange.PeerID,
		Target:  event.PeerChange.Region.Clone(),
	}, true
}

func PendingPeerChangeMatchesEvent(change PendingPeerChange, event rootevent.Event) bool {
	if event.PeerChange == nil {
		return false
	}
	expected, ok := PendingPeerChangeFromEvent(event)
	if !ok {
		return false
	}
	return change.Kind == expected.Kind &&
		change.StoreID == expected.StoreID &&
		change.PeerID == expected.PeerID &&
		change.Target.Equal(expected.Target)
}

func (c PeerChangeCompletion) Open() bool {
	return c.State == PeerChangeCompletionOpen
}

func (c PeerChangeCompletion) PendingState() bool {
	return c.State == PeerChangeCompletionPending
}

func (c PeerChangeCompletion) Completed() bool {
	return c.State == PeerChangeCompletionCompleted
}

func (l PeerChangeLifecycle) Retryable() bool {
	return l.Conflict
}

func ObservePeerChangeCompletion(pendingPeerChanges map[uint64]PendingPeerChange, current descriptor.Descriptor, hasCurrent bool, event rootevent.Event) PeerChangeCompletion {
	if event.PeerChange == nil {
		return PeerChangeCompletion{State: PeerChangeCompletionOpen}
	}
	if pending, exists := pendingPeerChanges[event.PeerChange.RegionID]; exists && PendingPeerChangeMatchesEvent(pending, event) {
		return PeerChangeCompletion{
			RegionID: event.PeerChange.RegionID,
			State:    PeerChangeCompletionPending,
			Pending:  pending,
		}
	}
	if hasCurrent && current.Equal(event.PeerChange.Region) {
		return PeerChangeCompletion{
			RegionID: event.PeerChange.RegionID,
			State:    PeerChangeCompletionCompleted,
		}
	}
	return PeerChangeCompletion{
		RegionID: event.PeerChange.RegionID,
		State:    PeerChangeCompletionOpen,
	}
}

func ObservePeerChangeLifecycle(pendingPeerChanges map[uint64]PendingPeerChange, current descriptor.Descriptor, hasCurrent bool, event rootevent.Event) PeerChangeLifecycle {
	if event.PeerChange == nil {
		return PeerChangeLifecycle{Decision: PeerChangeLifecycleApply}
	}
	completion := ObservePeerChangeCompletion(pendingPeerChanges, current, hasCurrent, event)
	_, pending := pendingPeerChanges[event.PeerChange.RegionID]
	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerRemovalPlanned:
		switch {
		case completion.Completed(), completion.PendingState():
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleSkip,
				Completion: completion,
			}
		default:
			if !pending {
				return PeerChangeLifecycle{
					Decision:   PeerChangeLifecycleApply,
					Completion: completion,
				}
			}
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
				Conflict:   true,
			}
		}
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		switch {
		case completion.PendingState():
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
			}
		case completion.Completed():
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleSkip,
				Completion: completion,
			}
		case pending:
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
				Conflict:   true,
			}
		default:
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
			}
		}
	}
	return PeerChangeLifecycle{
		Decision:   PeerChangeLifecycleApply,
		Completion: completion,
	}
}

func EvaluatePeerChangeLifecycle(pendingPeerChanges map[uint64]PendingPeerChange, current descriptor.Descriptor, hasCurrent bool, event rootevent.Event) (PeerChangeLifecycleDecision, error) {
	if event.PeerChange == nil {
		return PeerChangeLifecycleApply, nil
	}
	outcome := ObservePeerChangeLifecycle(pendingPeerChanges, current, hasCurrent, event)
	if !outcome.Conflict {
		return outcome.Decision, nil
	}
	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerRemovalPlanned:
		return outcome.Decision, fmt.Errorf("pending peer change already exists for region %d", event.PeerChange.RegionID)
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		return outcome.Decision, fmt.Errorf("peer change apply does not match pending target for region %d", event.PeerChange.RegionID)
	default:
		return outcome.Decision, nil
	}
}
