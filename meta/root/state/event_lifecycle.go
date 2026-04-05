package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

type RootEventLifecycleDecision uint8

const (
	RootEventLifecycleApply RootEventLifecycleDecision = iota
	RootEventLifecycleSkip
)

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
