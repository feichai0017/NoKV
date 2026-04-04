package view

import (
	"slices"
	"sync"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// TransitionSnapshot captures rooted pending transition state materialized into
// PD runtime view.
type TransitionSnapshot struct {
	PendingPeerChanges  map[uint64]rootstate.PendingPeerChange
	PendingRangeChanges map[uint64]rootstate.PendingRangeChange
	Entries             []TransitionEntry
}

// TransitionEntry is one rooted transition currently visible to PD operator and
// debugging surfaces.
type TransitionEntry struct {
	Kind        rootstate.TransitionKind
	Key         uint64
	Status      rootstate.TransitionStatus
	RetryClass  rootstate.TransitionRetryClass
	Reason      rootstate.TransitionReason
	PeerChange  *rootstate.PendingPeerChange
	RangeChange *rootstate.PendingRangeChange
}

// TransitionAssessment is one explicit lifecycle assessment for a proposed
// rooted transition event against the current rooted snapshot.
type TransitionAssessment struct {
	Kind       rootstate.TransitionKind
	Key        uint64
	Status     rootstate.TransitionStatus
	RetryClass rootstate.TransitionRetryClass
	Reason     rootstate.TransitionReason
	Decision   rootstate.RootEventLifecycleDecision
}

// TransitionView is the disposable runtime view of rooted pending execution
// state tracked by PD.
type TransitionView struct {
	mu                 sync.RWMutex
	pendingPeerChanges map[uint64]rootstate.PendingPeerChange
	pendingRangeChange map[uint64]rootstate.PendingRangeChange
	entries            []TransitionEntry
}

func NewTransitionView() *TransitionView {
	return &TransitionView{
		pendingPeerChanges: make(map[uint64]rootstate.PendingPeerChange),
		pendingRangeChange: make(map[uint64]rootstate.PendingRangeChange),
	}
}

func (v *TransitionView) Replace(descriptors map[uint64]descriptor.Descriptor, peers map[uint64]rootstate.PendingPeerChange, ranges map[uint64]rootstate.PendingRangeChange) {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.pendingPeerChanges = rootstate.ClonePendingPeerChanges(peers)
	v.pendingRangeChange = rootstate.ClonePendingRangeChanges(ranges)
	v.entries = buildTransitionEntries(descriptors, v.pendingPeerChanges, v.pendingRangeChange)
	v.mu.Unlock()
}

func (v *TransitionView) Snapshot() TransitionSnapshot {
	if v == nil {
		return TransitionSnapshot{
			PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
			PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
			Entries:             nil,
		}
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return TransitionSnapshot{
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(v.pendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(v.pendingRangeChange),
		Entries:             cloneTransitionEntries(v.entries),
	}
}

func AssessTransition(descriptors map[uint64]descriptor.Descriptor, peers map[uint64]rootstate.PendingPeerChange, ranges map[uint64]rootstate.PendingRangeChange, event rootevent.Event) TransitionAssessment {
	lifecycle := rootstate.ObserveRootEventLifecycle(rootstate.Snapshot{
		Descriptors:         rootstate.CloneDescriptors(descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(peers),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(ranges),
	}, event)
	return TransitionAssessment{
		Kind:       lifecycle.Kind,
		Key:        lifecycle.Key,
		Status:     lifecycle.Status,
		RetryClass: lifecycle.RetryClass,
		Reason:     lifecycle.Reason,
		Decision:   lifecycle.Decision,
	}
}

func buildTransitionEntries(descriptors map[uint64]descriptor.Descriptor, peers map[uint64]rootstate.PendingPeerChange, ranges map[uint64]rootstate.PendingRangeChange) []TransitionEntry {
	total := len(peers) + len(ranges)
	if total == 0 {
		return nil
	}
	entries := make([]TransitionEntry, 0, total)
	peerIDs := make([]uint64, 0, len(peers))
	for id := range peers {
		peerIDs = append(peerIDs, id)
	}
	slices.Sort(peerIDs)
	for _, id := range peerIDs {
		change := peers[id]
		event := peerChangePendingEvent(id, change)
		lifecycle := rootstate.ObserveRootEventLifecycle(rootstate.Snapshot{
			Descriptors:         descriptors,
			PendingPeerChanges:  peers,
			PendingRangeChanges: ranges,
		}, event)
		changeCopy := change
		entries = append(entries, TransitionEntry{
			Kind:       lifecycle.Kind,
			Key:        id,
			Status:     lifecycle.Status,
			RetryClass: lifecycle.RetryClass,
			Reason:     lifecycle.Reason,
			PeerChange: &changeCopy,
		})
	}
	rangeIDs := make([]uint64, 0, len(ranges))
	for id := range ranges {
		rangeIDs = append(rangeIDs, id)
	}
	slices.Sort(rangeIDs)
	for _, id := range rangeIDs {
		change := ranges[id]
		event := rangeChangePendingEvent(change)
		lifecycle := rootstate.ObserveRootEventLifecycle(rootstate.Snapshot{
			Descriptors:         descriptors,
			PendingPeerChanges:  peers,
			PendingRangeChanges: ranges,
		}, event)
		changeCopy := change
		entries = append(entries, TransitionEntry{
			Kind:        lifecycle.Kind,
			Key:         id,
			Status:      lifecycle.Status,
			RetryClass:  lifecycle.RetryClass,
			Reason:      lifecycle.Reason,
			RangeChange: &changeCopy,
		})
	}
	return entries
}

func peerChangePendingEvent(regionID uint64, change rootstate.PendingPeerChange) rootevent.Event {
	switch change.Kind {
	case rootstate.PendingPeerChangeAddition:
		return rootevent.PeerAdditionPlanned(regionID, change.StoreID, change.PeerID, change.Target)
	case rootstate.PendingPeerChangeRemoval:
		return rootevent.PeerRemovalPlanned(regionID, change.StoreID, change.PeerID, change.Target)
	default:
		return rootevent.Event{}
	}
}

func rangeChangePendingEvent(change rootstate.PendingRangeChange) rootevent.Event {
	switch change.Kind {
	case rootstate.PendingRangeChangeSplit:
		return rootevent.RegionSplitPlanned(change.ParentRegionID, change.Right.StartKey, change.Left, change.Right)
	case rootstate.PendingRangeChangeMerge:
		return rootevent.RegionMergePlanned(change.LeftRegionID, change.RightRegionID, change.Merged)
	default:
		return rootevent.Event{}
	}
}

func cloneTransitionEntries(in []TransitionEntry) []TransitionEntry {
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
