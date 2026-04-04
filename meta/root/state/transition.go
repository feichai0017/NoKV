package state

import (
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
	// TransitionStatusCancelled is reserved for future explicit withdrawn plans.
	TransitionStatusCancelled
	TransitionStatusAborted
)

// TransitionRetryClass explains whether one rejected/stalled transition is
// retryable and why.
type TransitionRetryClass uint8

const (
	TransitionRetryNone TransitionRetryClass = iota
	TransitionRetryConflict
	TransitionRetryTransient
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

func (l RootEventLifecycle) Retryable() bool {
	return l.RetryClass != TransitionRetryNone
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
