package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

type RootEventLifecycleDecision uint8

const (
	RootEventLifecycleApply RootEventLifecycleDecision = iota
	RootEventLifecycleSkip
)

func EvaluateRootEventLifecycle(snapshot Snapshot, event rootevent.Event) (RootEventLifecycleDecision, error) {
	if event.PeerChange != nil {
		current, ok := snapshot.Descriptors[event.PeerChange.RegionID]
		decision, err := EvaluatePeerChangeLifecycle(snapshot.PendingPeerChanges, current, ok, event)
		if decision == PeerChangeLifecycleSkip {
			return RootEventLifecycleSkip, err
		}
		return RootEventLifecycleApply, err
	}
	if event.RangeSplit != nil || event.RangeMerge != nil {
		decision, err := EvaluateRangeChangeLifecycle(snapshot.PendingRangeChanges, snapshot.Descriptors, event)
		if decision == RangeChangeLifecycleSkip {
			return RootEventLifecycleSkip, err
		}
		return RootEventLifecycleApply, err
	}
	return RootEventLifecycleApply, nil
}
