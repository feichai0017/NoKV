package state

import (
	"slices"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

// TransitionEntry is one rooted transition currently visible to operator and
// debugging surfaces.
type TransitionEntry struct {
	Kind        TransitionKind
	Key         uint64
	Status      TransitionStatus
	RetryClass  TransitionRetryClass
	Reason      TransitionReason
	PeerChange  *PendingPeerChange
	RangeChange *PendingRangeChange
}

// TransitionAssessment is one explicit lifecycle assessment for a proposed
// rooted transition event against the current rooted snapshot.
type TransitionAssessment struct {
	Kind       TransitionKind
	Key        uint64
	Status     TransitionStatus
	RetryClass TransitionRetryClass
	Reason     TransitionReason
	Decision   RootEventLifecycleDecision
}

// AssessTransition evaluates one explicit rooted transition event against the
// supplied rooted snapshot without mutating it.
func AssessTransition(snapshot Snapshot, event rootevent.Event) TransitionAssessment {
	lifecycle := ObserveRootEventLifecycle(snapshot, event)
	return TransitionAssessment(lifecycle)
}

// BuildTransitionEntries projects the rooted pending transition maps into one
// stable operator/debug surface.
func BuildTransitionEntries(snapshot Snapshot) []TransitionEntry {
	total := len(snapshot.PendingPeerChanges) + len(snapshot.PendingRangeChanges)
	if total == 0 {
		return nil
	}
	entries := make([]TransitionEntry, 0, total)

	peerIDs := make([]uint64, 0, len(snapshot.PendingPeerChanges))
	for id := range snapshot.PendingPeerChanges {
		peerIDs = append(peerIDs, id)
	}
	slices.Sort(peerIDs)
	for _, id := range peerIDs {
		entries = append(entries, transitionEntryFromPendingPeerChange(snapshot, id, snapshot.PendingPeerChanges[id]))
	}

	rangeIDs := make([]uint64, 0, len(snapshot.PendingRangeChanges))
	for id := range snapshot.PendingRangeChanges {
		rangeIDs = append(rangeIDs, id)
	}
	slices.Sort(rangeIDs)
	for _, id := range rangeIDs {
		entries = append(entries, transitionEntryFromPendingRangeChange(snapshot, id, snapshot.PendingRangeChanges[id]))
	}
	return entries
}

// CloneTransitionEntries returns a detached transition-entry slice.
func CloneTransitionEntries(in []TransitionEntry) []TransitionEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]TransitionEntry, 0, len(in))
	for _, entry := range in {
		item := entry
		if entry.PeerChange != nil {
			change := *entry.PeerChange
			item.PeerChange = &change
		}
		if entry.RangeChange != nil {
			change := *entry.RangeChange
			item.RangeChange = &change
		}
		out = append(out, item)
	}
	return out
}

func transitionEntryFromPendingPeerChange(snapshot Snapshot, regionID uint64, change PendingPeerChange) TransitionEntry {
	current, ok := snapshot.Descriptors[regionID]
	lifecycle := ObservePeerChangeLifecycle(snapshot.PendingPeerChanges, current, ok, pendingPeerChangeEvent(regionID, change))
	changeCopy := change
	return TransitionEntry{
		Kind:       TransitionKindPeerChange,
		Key:        regionID,
		Status:     lifecycle.Status,
		RetryClass: lifecycle.RetryClass,
		Reason:     lifecycle.Reason,
		PeerChange: &changeCopy,
	}
}

func transitionEntryFromPendingRangeChange(snapshot Snapshot, key uint64, change PendingRangeChange) TransitionEntry {
	lifecycle := ObserveRangeChangeLifecycle(snapshot.PendingRangeChanges, snapshot.Descriptors, pendingRangeChangeEvent(change))
	changeCopy := change
	return TransitionEntry{
		Kind:        TransitionKindRangeChange,
		Key:         key,
		Status:      lifecycle.Status,
		RetryClass:  lifecycle.RetryClass,
		Reason:      lifecycle.Reason,
		RangeChange: &changeCopy,
	}
}

func pendingPeerChangeEvent(regionID uint64, change PendingPeerChange) rootevent.Event {
	switch change.Kind {
	case PendingPeerChangeAddition:
		return rootevent.PeerAdditionPlanned(regionID, change.StoreID, change.PeerID, change.Target)
	case PendingPeerChangeRemoval:
		return rootevent.PeerRemovalPlanned(regionID, change.StoreID, change.PeerID, change.Target)
	default:
		return rootevent.Event{}
	}
}

func pendingRangeChangeEvent(change PendingRangeChange) rootevent.Event {
	switch change.Kind {
	case PendingRangeChangeSplit:
		return rootevent.RegionSplitPlanned(change.ParentRegionID, change.Right.StartKey, change.Left, change.Right)
	case PendingRangeChangeMerge:
		return rootevent.RegionMergePlanned(change.LeftRegionID, change.RightRegionID, change.Merged)
	default:
		return rootevent.Event{}
	}
}
