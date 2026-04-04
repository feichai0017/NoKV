package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// ApplyEventToSnapshot applies one rooted metadata event into the compact
// rooted snapshot, including pending execution state.
func ApplyEventToSnapshot(snapshot *Snapshot, cursor Cursor, event rootevent.Event) {
	if snapshot == nil {
		return
	}
	if snapshot.Descriptors == nil {
		snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	if snapshot.PendingPeerChanges == nil {
		snapshot.PendingPeerChanges = make(map[uint64]PendingPeerChange)
	}
	if snapshot.PendingRangeChanges == nil {
		snapshot.PendingRangeChanges = make(map[uint64]PendingRangeChange)
	}
	switch event.Kind {
	case rootevent.KindStoreJoined, rootevent.KindStoreLeft:
		snapshot.State.MembershipEpoch++
	case rootevent.KindIDAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > snapshot.State.IDFence {
			snapshot.State.IDFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTSOAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > snapshot.State.TSOFence {
			snapshot.State.TSOFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindRegionBootstrap, rootevent.KindRegionDescriptorPublished:
		snapshot.State.ClusterEpoch++
		desc := event.RegionDescriptor.Descriptor.Clone()
		snapshot.Descriptors[desc.RegionID] = desc
		delete(snapshot.PendingPeerChanges, desc.RegionID)
		delete(snapshot.PendingRangeChanges, desc.RegionID)
	case rootevent.KindRegionTombstoned:
		snapshot.State.ClusterEpoch++
		delete(snapshot.Descriptors, event.RegionRemoval.RegionID)
		delete(snapshot.PendingPeerChanges, event.RegionRemoval.RegionID)
		delete(snapshot.PendingRangeChanges, event.RegionRemoval.RegionID)
	default:
		if ApplyRangeChangeToSnapshot(snapshot, event) {
			break
		}
		_ = ApplyPeerChangeToSnapshot(snapshot, event)
	}
	snapshot.State.LastCommitted = cursor
}

// ApplyPeerChangeToSnapshot applies one peer-change lifecycle event into the
// compact rooted snapshot. It returns false when the event is not a peer-change
// lifecycle event.
func ApplyPeerChangeToSnapshot(snapshot *Snapshot, event rootevent.Event) bool {
	if snapshot == nil || event.PeerChange == nil {
		return false
	}
	if snapshot.Descriptors == nil {
		snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	if snapshot.PendingPeerChanges == nil {
		snapshot.PendingPeerChanges = make(map[uint64]PendingPeerChange)
	}

	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerRemovalPlanned:
		snapshot.State.ClusterEpoch++
		change, _ := PendingPeerChangeFromEvent(event)
		snapshot.Descriptors[event.PeerChange.RegionID] = change.Target.Clone()
		snapshot.PendingPeerChanges[event.PeerChange.RegionID] = change
		return true
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		change, _ := PendingPeerChangeFromEvent(event)
		prev, ok := snapshot.PendingPeerChanges[event.PeerChange.RegionID]
		matchedPending := ok && PendingPeerChangeMatchesEvent(prev, event)
		if !matchedPending {
			snapshot.State.ClusterEpoch++
		}
		snapshot.Descriptors[event.PeerChange.RegionID] = change.Target.Clone()
		delete(snapshot.PendingPeerChanges, event.PeerChange.RegionID)
		return true
	default:
		return false
	}
}

// ApplyRangeChangeToSnapshot applies one split/merge lifecycle event into the
// compact rooted snapshot. It returns false when the event is not a range-change
// lifecycle event.
func ApplyRangeChangeToSnapshot(snapshot *Snapshot, event rootevent.Event) bool {
	if snapshot == nil {
		return false
	}
	if snapshot.Descriptors == nil {
		snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	if snapshot.PendingPeerChanges == nil {
		snapshot.PendingPeerChanges = make(map[uint64]PendingPeerChange)
	}
	if snapshot.PendingRangeChanges == nil {
		snapshot.PendingRangeChanges = make(map[uint64]PendingRangeChange)
	}

	switch event.Kind {
	case rootevent.KindRegionSplitPlanned, rootevent.KindRegionSplitCommitted:
		key, change, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		return applyRangeChangeSnapshotMutation(snapshot, event, key, change, func(snapshot *Snapshot) {
			delete(snapshot.Descriptors, event.RangeSplit.ParentRegionID)
			delete(snapshot.PendingPeerChanges, event.RangeSplit.ParentRegionID)
			snapshot.Descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
			snapshot.Descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
		})
	case rootevent.KindRegionMergePlanned, rootevent.KindRegionMerged:
		key, change, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		return applyRangeChangeSnapshotMutation(snapshot, event, key, change, func(snapshot *Snapshot) {
			delete(snapshot.Descriptors, event.RangeMerge.LeftRegionID)
			delete(snapshot.Descriptors, event.RangeMerge.RightRegionID)
			delete(snapshot.PendingPeerChanges, event.RangeMerge.LeftRegionID)
			delete(snapshot.PendingPeerChanges, event.RangeMerge.RightRegionID)
			delete(snapshot.PendingRangeChanges, event.RangeMerge.LeftRegionID)
			delete(snapshot.PendingRangeChanges, event.RangeMerge.RightRegionID)
			snapshot.Descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
		})
	default:
		return false
	}
}

func applyRangeChangeSnapshotMutation(snapshot *Snapshot, event rootevent.Event, key uint64, change PendingRangeChange, mutate func(*Snapshot)) bool {
	completion := ObserveRangeChangeCompletion(snapshot.PendingRangeChanges, snapshot.Descriptors, event)
	if isPlannedRangeChangeEvent(event) || completion == RangeChangeCompletionOpen {
		snapshot.State.ClusterEpoch++
	}
	mutate(snapshot)
	if isPlannedRangeChangeEvent(event) {
		snapshot.PendingRangeChanges[key] = change
		return true
	}
	delete(snapshot.PendingRangeChanges, key)
	return true
}

func isPlannedRangeChangeEvent(event rootevent.Event) bool {
	return event.Kind == rootevent.KindRegionSplitPlanned || event.Kind == rootevent.KindRegionMergePlanned
}
