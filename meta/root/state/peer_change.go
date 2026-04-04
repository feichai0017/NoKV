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

func ObservePeerChangeCompletion(pendingPeerChanges map[uint64]PendingPeerChange, current descriptor.Descriptor, hasCurrent bool, event rootevent.Event) PeerChangeCompletionState {
	if event.PeerChange == nil {
		return PeerChangeCompletionOpen
	}
	if pending, exists := pendingPeerChanges[event.PeerChange.RegionID]; exists && PendingPeerChangeMatchesEvent(pending, event) {
		return PeerChangeCompletionPending
	}
	if hasCurrent && current.Equal(event.PeerChange.Region) {
		return PeerChangeCompletionCompleted
	}
	return PeerChangeCompletionOpen
}

func EvaluatePeerChangeLifecycle(pendingPeerChanges map[uint64]PendingPeerChange, current descriptor.Descriptor, hasCurrent bool, event rootevent.Event) (PeerChangeLifecycleDecision, error) {
	if event.PeerChange == nil {
		return PeerChangeLifecycleApply, nil
	}
	change, pending := pendingPeerChanges[event.PeerChange.RegionID]
	completion := ObservePeerChangeCompletion(pendingPeerChanges, current, hasCurrent, event)
	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerRemovalPlanned:
		switch completion {
		case PeerChangeCompletionCompleted, PeerChangeCompletionPending:
			return PeerChangeLifecycleSkip, nil
		default:
			if !pending {
				return PeerChangeLifecycleApply, nil
			}
			return PeerChangeLifecycleApply, fmt.Errorf("pending peer change already exists for region %d", event.PeerChange.RegionID)
		}
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		switch completion {
		case PeerChangeCompletionPending:
			return PeerChangeLifecycleApply, nil
		case PeerChangeCompletionCompleted:
			return PeerChangeLifecycleSkip, nil
		default:
			if pending {
				if PendingPeerChangeMatchesEvent(change, event) {
					return PeerChangeLifecycleApply, nil
				}
				return PeerChangeLifecycleApply, fmt.Errorf("peer change apply does not match pending target for region %d", event.PeerChange.RegionID)
			}
			return PeerChangeLifecycleApply, nil
		}
	}
	return PeerChangeLifecycleApply, nil
}
