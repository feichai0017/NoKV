package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

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
		if event.Kind == rootevent.KindRegionSplitPlanned || !RangeChangeStateMatches(snapshot.Descriptors, event) {
			snapshot.State.ClusterEpoch++
		}
		delete(snapshot.Descriptors, event.RangeSplit.ParentRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeSplit.ParentRegionID)
		snapshot.Descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
		snapshot.Descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
		if event.Kind == rootevent.KindRegionSplitPlanned {
			snapshot.PendingRangeChanges[key] = change
		} else {
			delete(snapshot.PendingRangeChanges, key)
		}
		return true
	case rootevent.KindRegionMergePlanned, rootevent.KindRegionMerged:
		key, change, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		if event.Kind == rootevent.KindRegionMergePlanned || !RangeChangeStateMatches(snapshot.Descriptors, event) {
			snapshot.State.ClusterEpoch++
		}
		delete(snapshot.Descriptors, event.RangeMerge.LeftRegionID)
		delete(snapshot.Descriptors, event.RangeMerge.RightRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.LeftRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.RightRegionID)
		delete(snapshot.PendingRangeChanges, event.RangeMerge.LeftRegionID)
		delete(snapshot.PendingRangeChanges, event.RangeMerge.RightRegionID)
		snapshot.Descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
		if event.Kind == rootevent.KindRegionMergePlanned {
			snapshot.PendingRangeChanges[key] = change
		} else {
			delete(snapshot.PendingRangeChanges, key)
		}
		return true
	default:
		return false
	}
}
