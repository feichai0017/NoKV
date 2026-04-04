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
	Status     TransitionStatus
	RetryClass TransitionRetryClass
	Reason     TransitionReason
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
	return l.RetryClass != TransitionRetryNone
}

func peerChangeSuperseded(current descriptor.Descriptor, hasCurrent bool, event rootevent.Event) bool {
	if event.PeerChange == nil || !hasCurrent {
		return false
	}
	target := event.PeerChange.Region
	if current.Equal(target) {
		return false
	}
	return target.RootEpoch != 0 && current.RootEpoch > target.RootEpoch
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
		return PeerChangeLifecycle{
			Decision: PeerChangeLifecycleApply,
			Status:   TransitionStatusOpen,
			Reason:   TransitionReasonOpenApply,
		}
	}
	completion := ObservePeerChangeCompletion(pendingPeerChanges, current, hasCurrent, event)
	_, pending := pendingPeerChanges[event.PeerChange.RegionID]
	superseded := peerChangeSuperseded(current, hasCurrent, event)
	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerRemovalPlanned:
		switch {
		case completion.Completed(), completion.PendingState():
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleSkip,
				Completion: completion,
				Status:     completionStatus(completion),
				Reason:     completionReason(completion),
			}
		case superseded:
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleSkip,
				Completion: completion,
				Status:     TransitionStatusSuperseded,
				Reason:     TransitionReasonSupersededTarget,
			}
		default:
			if !pending {
				return PeerChangeLifecycle{
					Decision:   PeerChangeLifecycleApply,
					Completion: completion,
					Status:     TransitionStatusOpen,
					Reason:     TransitionReasonOpenApply,
				}
			}
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
				Status:     TransitionStatusConflict,
				RetryClass: TransitionRetryConflict,
				Reason:     TransitionReasonConflictingPending,
				Conflict:   true,
			}
		}
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		switch {
		case completion.PendingState():
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
				Status:     TransitionStatusPending,
				Reason:     TransitionReasonMatchingPending,
			}
		case completion.Completed():
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleSkip,
				Completion: completion,
				Status:     TransitionStatusCompleted,
				Reason:     TransitionReasonAlreadyCompleted,
			}
		case superseded:
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleSkip,
				Completion: completion,
				Status:     TransitionStatusAborted,
				Reason:     TransitionReasonAbortedApply,
			}
		case pending:
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
				Status:     TransitionStatusConflict,
				RetryClass: TransitionRetryConflict,
				Reason:     TransitionReasonConflictingPending,
				Conflict:   true,
			}
		default:
			return PeerChangeLifecycle{
				Decision:   PeerChangeLifecycleApply,
				Completion: completion,
				Status:     TransitionStatusOpen,
				Reason:     TransitionReasonOpenApply,
			}
		}
	}
	return PeerChangeLifecycle{
		Decision:   PeerChangeLifecycleApply,
		Completion: completion,
		Status:     TransitionStatusOpen,
		Reason:     TransitionReasonOpenApply,
	}
}

func EvaluatePeerChangeLifecycle(pendingPeerChanges map[uint64]PendingPeerChange, current descriptor.Descriptor, hasCurrent bool, event rootevent.Event) (PeerChangeLifecycleDecision, error) {
	if event.PeerChange == nil {
		return PeerChangeLifecycleApply, nil
	}
	outcome := ObservePeerChangeLifecycle(pendingPeerChanges, current, hasCurrent, event)
	if !outcome.Conflict {
		if outcome.Status == TransitionStatusAborted {
			return outcome.Decision, fmt.Errorf("peer change target for region %d was superseded before apply", event.PeerChange.RegionID)
		}
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

func completionStatus(completion PeerChangeCompletion) TransitionStatus {
	switch completion.State {
	case PeerChangeCompletionPending:
		return TransitionStatusPending
	case PeerChangeCompletionCompleted:
		return TransitionStatusCompleted
	default:
		return TransitionStatusOpen
	}
}

func completionReason(completion PeerChangeCompletion) TransitionReason {
	switch completion.State {
	case PeerChangeCompletionPending:
		return TransitionReasonMatchingPending
	case PeerChangeCompletionCompleted:
		return TransitionReasonAlreadyCompleted
	default:
		return TransitionReasonOpenApply
	}
}
