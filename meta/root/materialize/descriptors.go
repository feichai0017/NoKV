package materialize

import (
	"slices"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
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
	case event.PeerChange != nil && (event.Kind == rootevent.KindPeerAdditionCancelled || event.Kind == rootevent.KindPeerRemovalCancelled):
		if event.PeerChange.Base.RegionID == 0 {
			delete(descriptors, event.PeerChange.RegionID)
			return
		}
		descriptors[event.PeerChange.Base.RegionID] = event.PeerChange.Base.Clone()
	case event.RangeSplit != nil && event.Kind == rootevent.KindRegionSplitCancelled:
		delete(descriptors, event.RangeSplit.Left.RegionID)
		delete(descriptors, event.RangeSplit.Right.RegionID)
		if event.RangeSplit.BaseParent.RegionID != 0 {
			descriptors[event.RangeSplit.BaseParent.RegionID] = event.RangeSplit.BaseParent.Clone()
		}
	case event.RangeMerge != nil && event.Kind == rootevent.KindRegionMergeCancelled:
		delete(descriptors, event.RangeMerge.Merged.RegionID)
		if event.RangeMerge.BaseLeft.RegionID != 0 {
			descriptors[event.RangeMerge.BaseLeft.RegionID] = event.RangeMerge.BaseLeft.Clone()
		}
		if event.RangeMerge.BaseRight.RegionID != 0 {
			descriptors[event.RangeMerge.BaseRight.RegionID] = event.RangeMerge.BaseRight.Clone()
		}
	case event.RangeSplit != nil:
		delete(descriptors, event.RangeSplit.ParentRegionID)
		descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
		descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
	case event.RangeMerge != nil:
		delete(descriptors, event.RangeMerge.LeftRegionID)
		delete(descriptors, event.RangeMerge.RightRegionID)
		descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
	case event.PeerChange != nil:
		descriptors[event.PeerChange.RegionID] = event.PeerChange.Region.Clone()
	}
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
	slices.Sort(ids)
	events := make([]rootevent.Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, rootevent.RegionDescriptorPublished(descs[id]))
	}
	return events
}
