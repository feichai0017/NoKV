package materialize

import (
	"sort"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// ApplyEventToDescriptors applies one rooted topology event into a materialized descriptor catalog.
func ApplyEventToDescriptors(descriptors map[uint64]descriptor.Descriptor, event rootevent.Event) {
	if descriptors == nil {
		return
	}
	switch {
	case event.RegionDescriptor != nil:
		descriptors[event.RegionDescriptor.Descriptor.RegionID] = event.RegionDescriptor.Descriptor.Clone()
	case event.RegionRemoval != nil:
		delete(descriptors, event.RegionRemoval.RegionID)
	case event.RangeSplit != nil:
		delete(descriptors, event.RangeSplit.ParentRegionID)
		descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
		descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
	case event.RangeMerge != nil:
		delete(descriptors, event.RangeMerge.LeftRegionID)
		delete(descriptors, event.RangeMerge.RightRegionID)
		descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
	case event.PeerChange != nil:
		descriptors[event.PeerChange.Region.RegionID] = event.PeerChange.Region.Clone()
	}
}

// ApplyEventToSnapshot applies one rooted metadata event into the compact
// rooted snapshot, including pending peer-change execution state.
func ApplyEventToSnapshot(snapshot *rootstate.Snapshot, cursor rootstate.Cursor, event rootevent.Event) {
	if snapshot == nil {
		return
	}
	if snapshot.Descriptors == nil {
		snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	if snapshot.PendingPeerChanges == nil {
		snapshot.PendingPeerChanges = make(map[uint64]rootstate.PendingPeerChange)
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
	case rootevent.KindRegionTombstoned:
		snapshot.State.ClusterEpoch++
		delete(snapshot.Descriptors, event.RegionRemoval.RegionID)
		delete(snapshot.PendingPeerChanges, event.RegionRemoval.RegionID)
	case rootevent.KindRegionSplitPlanned, rootevent.KindRegionSplitCommitted:
		if event.Kind == rootevent.KindRegionSplitPlanned ||
			!splitStateMatches(snapshot.Descriptors, event.RangeSplit.ParentRegionID, event.RangeSplit.Left, event.RangeSplit.Right) {
			snapshot.State.ClusterEpoch++
		}
		delete(snapshot.Descriptors, event.RangeSplit.ParentRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeSplit.ParentRegionID)
		snapshot.Descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
		snapshot.Descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
	case rootevent.KindRegionMergePlanned, rootevent.KindRegionMerged:
		if event.Kind == rootevent.KindRegionMergePlanned ||
			!mergeStateMatches(snapshot.Descriptors, event.RangeMerge.LeftRegionID, event.RangeMerge.RightRegionID, event.RangeMerge.Merged) {
			snapshot.State.ClusterEpoch++
		}
		delete(snapshot.Descriptors, event.RangeMerge.LeftRegionID)
		delete(snapshot.Descriptors, event.RangeMerge.RightRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.LeftRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.RightRegionID)
		snapshot.Descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerRemovalPlanned:
		snapshot.State.ClusterEpoch++
		change, _ := rootstate.PendingPeerChangeFromEvent(event)
		snapshot.Descriptors[event.PeerChange.RegionID] = change.Target.Clone()
		snapshot.PendingPeerChanges[event.PeerChange.RegionID] = change
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		change, _ := rootstate.PendingPeerChangeFromEvent(event)
		prev, ok := snapshot.PendingPeerChanges[event.PeerChange.RegionID]
		matchedPending := ok && rootstate.PendingPeerChangeMatchesEvent(prev, event)
		if !matchedPending {
			snapshot.State.ClusterEpoch++
		}
		snapshot.Descriptors[event.PeerChange.RegionID] = change.Target.Clone()
		delete(snapshot.PendingPeerChanges, event.PeerChange.RegionID)
	}
	snapshot.State.LastCommitted = cursor
}

func splitStateMatches(descriptors map[uint64]descriptor.Descriptor, parentRegionID uint64, left, right descriptor.Descriptor) bool {
	if _, ok := descriptors[parentRegionID]; ok {
		return false
	}
	gotLeft, ok := descriptors[left.RegionID]
	if !ok || !gotLeft.Equal(left) {
		return false
	}
	gotRight, ok := descriptors[right.RegionID]
	return ok && gotRight.Equal(right)
}

func mergeStateMatches(descriptors map[uint64]descriptor.Descriptor, leftRegionID, rightRegionID uint64, merged descriptor.Descriptor) bool {
	if _, ok := descriptors[leftRegionID]; ok {
		return false
	}
	if _, ok := descriptors[rightRegionID]; ok {
		return false
	}
	gotMerged, ok := descriptors[merged.RegionID]
	return ok && gotMerged.Equal(merged)
}

// SnapshotDescriptorEvents materializes descriptor truth into a stable event sequence for bootstrap/recovery callers.
func SnapshotDescriptorEvents(descs map[uint64]descriptor.Descriptor) []rootevent.Event {
	if len(descs) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(descs))
	for id := range descs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	events := make([]rootevent.Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, rootevent.RegionDescriptorPublished(descs[id]))
	}
	return events
}
