package state

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

// TransitionKind classifies rooted topology transition families.
type TransitionKind uint8

const (
	TransitionKindUnknown TransitionKind = iota
	TransitionKindPeerChange
	TransitionKindRangeChange
)

// TransitionStatus describes the observable lifecycle state of one rooted
// transition target relative to the current rooted snapshot.
type TransitionStatus uint8

const (
	TransitionStatusOpen TransitionStatus = iota
	TransitionStatusPending
	TransitionStatusCompleted
	TransitionStatusConflict
	TransitionStatusSuperseded
	TransitionStatusCancelled
	TransitionStatusAborted
)

// TransitionRetryClass explains whether one rejected/stalled transition is
// retryable and why.
type TransitionRetryClass uint8

const (
	TransitionRetryNone TransitionRetryClass = iota
	TransitionRetryConflict
)

// TransitionReason provides a machine-readable explanation for one lifecycle
// observation.
type TransitionReason uint8

const (
	TransitionReasonNone TransitionReason = iota
	TransitionReasonOpenApply
	TransitionReasonMatchingPending
	TransitionReasonAlreadyCompleted
	TransitionReasonConflictingPending
	TransitionReasonSupersededTarget
	TransitionReasonCancelledTarget
	TransitionReasonAbortedApply
)

type RootEventLifecycleDecision uint8

const (
	RootEventLifecycleApply RootEventLifecycleDecision = iota
	RootEventLifecycleSkip
)

// RootEventLifecycle is the generic lifecycle observation returned for one
// explicit rooted transition event.
type RootEventLifecycle struct {
	Kind       TransitionKind
	Key        uint64
	Status     TransitionStatus
	RetryClass TransitionRetryClass
	Reason     TransitionReason
	Decision   RootEventLifecycleDecision
}

func rootEventLifecycleKey(event rootevent.Event) uint64 {
	switch {
	case event.PeerChange != nil:
		return event.PeerChange.RegionID
	case event.RangeSplit != nil:
		return event.RangeSplit.ParentRegionID
	case event.RangeMerge != nil:
		return event.RangeMerge.Merged.RegionID
	default:
		return 0
	}
}

func ObserveRootEventLifecycle(snapshot Snapshot, event rootevent.Event) RootEventLifecycle {
	if event.PeerChange != nil {
		current, ok := snapshot.Descriptors[event.PeerChange.RegionID]
		lifecycle := ObservePeerChangeLifecycle(snapshot.PendingPeerChanges, current, ok, event)
		return RootEventLifecycle{
			Kind:       TransitionKindPeerChange,
			Key:        event.PeerChange.RegionID,
			Status:     lifecycle.Status,
			RetryClass: lifecycle.RetryClass,
			Reason:     lifecycle.Reason,
			Decision:   rootEventDecisionFromPeer(lifecycle.Decision),
		}
	}
	if event.RangeSplit != nil || event.RangeMerge != nil {
		key := rootEventLifecycleKey(event)
		lifecycle := ObserveRangeChangeLifecycle(snapshot.PendingRangeChanges, snapshot.Descriptors, event)
		return RootEventLifecycle{
			Kind:       TransitionKindRangeChange,
			Key:        key,
			Status:     lifecycle.Status,
			RetryClass: lifecycle.RetryClass,
			Reason:     lifecycle.Reason,
			Decision:   rootEventDecisionFromRange(lifecycle.Decision),
		}
	}
	return RootEventLifecycle{
		Kind:     TransitionKindUnknown,
		Status:   TransitionStatusOpen,
		Reason:   TransitionReasonOpenApply,
		Decision: RootEventLifecycleApply,
	}
}

func EvaluateRootEventLifecycle(snapshot Snapshot, event rootevent.Event) (RootEventLifecycleDecision, error) {
	outcome := ObserveRootEventLifecycle(snapshot, event)
	if event.PeerChange != nil {
		current, ok := snapshot.Descriptors[event.PeerChange.RegionID]
		_, err := EvaluatePeerChangeLifecycle(snapshot.PendingPeerChanges, current, ok, event)
		return outcome.Decision, err
	}
	if event.RangeSplit != nil || event.RangeMerge != nil {
		_, err := EvaluateRangeChangeLifecycle(snapshot.PendingRangeChanges, snapshot.Descriptors, event)
		return outcome.Decision, err
	}
	return outcome.Decision, nil
}

func rootEventDecisionFromPeer(in PeerChangeLifecycleDecision) RootEventLifecycleDecision {
	if in == PeerChangeLifecycleSkip {
		return RootEventLifecycleSkip
	}
	return RootEventLifecycleApply
}

func rootEventDecisionFromRange(in RangeChangeLifecycleDecision) RootEventLifecycleDecision {
	if in == RangeChangeLifecycleSkip {
		return RootEventLifecycleSkip
	}
	return RootEventLifecycleApply
}

// TransitionIDFromEvent returns one stable identity for a rooted transition target.
func TransitionIDFromEvent(event rootevent.Event) string {
	switch {
	case event.PeerChange != nil:
		return fmt.Sprintf("peer:%d:%s:%d:%d", event.PeerChange.RegionID, peerTransitionAction(event.Kind), event.PeerChange.StoreID, event.PeerChange.PeerID)
	case event.RangeSplit != nil:
		return fmt.Sprintf("split:%d:%x", event.RangeSplit.ParentRegionID, event.RangeSplit.SplitKey)
	case event.RangeMerge != nil:
		return fmt.Sprintf("merge:%d:%d", event.RangeMerge.LeftRegionID, event.RangeMerge.RightRegionID)
	default:
		return ""
	}
}

func peerTransitionAction(kind rootevent.Kind) string {
	switch kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerAdded, rootevent.KindPeerAdditionCancelled:
		return "add"
	case rootevent.KindPeerRemovalPlanned, rootevent.KindPeerRemoved, rootevent.KindPeerRemovalCancelled:
		return "remove"
	default:
		return "unknown"
	}
}
