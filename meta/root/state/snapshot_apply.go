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
	if snapshot.Stores == nil {
		snapshot.Stores = make(map[uint64]StoreMembership)
	}
	if snapshot.SnapshotEpochs == nil {
		snapshot.SnapshotEpochs = make(map[string]SnapshotEpoch)
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
	case rootevent.KindStoreJoined:
		applyStoreJoinedToSnapshot(snapshot, cursor, event)
	case rootevent.KindStoreRetired:
		applyStoreRetiredToSnapshot(snapshot, cursor, event)
	case rootevent.KindSnapshotEpochPublished:
		applySnapshotEpochPublishedToSnapshot(snapshot, cursor, event)
	case rootevent.KindIDAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > snapshot.State.IDFence {
			snapshot.State.IDFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTSOAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > snapshot.State.TSOFence {
			snapshot.State.TSOFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTenure:
		applyTenureToState(&snapshot.State, cursor, event)
	case rootevent.KindLegacy:
		applyLegacyToState(&snapshot.State, cursor, event)
	case rootevent.KindHandover:
		applyHandoverToState(&snapshot.State, cursor, event)
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

func applySnapshotEpochPublishedToSnapshot(snapshot *Snapshot, cursor Cursor, event rootevent.Event) {
	if snapshot == nil || event.SnapshotEpoch == nil {
		return
	}
	epoch := SnapshotEpoch{
		SnapshotID:  event.SnapshotEpoch.SnapshotID,
		Mount:       event.SnapshotEpoch.Mount,
		RootInode:   event.SnapshotEpoch.RootInode,
		ReadVersion: event.SnapshotEpoch.ReadVersion,
		PublishedAt: cursor,
	}
	if epoch.SnapshotID == "" {
		epoch.SnapshotID = rootevent.SnapshotEpochID(epoch.Mount, epoch.RootInode, epoch.ReadVersion)
	}
	if epoch.Mount == "" || epoch.RootInode == 0 || epoch.ReadVersion == 0 {
		return
	}
	snapshot.SnapshotEpochs[epoch.SnapshotID] = epoch
}

func applyStoreJoinedToSnapshot(snapshot *Snapshot, cursor Cursor, event rootevent.Event) {
	if snapshot == nil || event.StoreMembership == nil || event.StoreMembership.StoreID == 0 {
		return
	}
	storeID := event.StoreMembership.StoreID
	snapshot.State.MembershipEpoch++
	snapshot.Stores[storeID] = StoreMembership{
		StoreID:  storeID,
		State:    StoreMembershipActive,
		JoinedAt: cursor,
	}
}

func applyStoreRetiredToSnapshot(snapshot *Snapshot, cursor Cursor, event rootevent.Event) {
	if snapshot == nil || event.StoreMembership == nil || event.StoreMembership.StoreID == 0 {
		return
	}
	storeID := event.StoreMembership.StoreID
	current := snapshot.Stores[storeID]
	if current.JoinedAt == (Cursor{}) {
		current.JoinedAt = cursor
	}
	current.StoreID = storeID
	current.State = StoreMembershipRetired
	current.RetiredAt = cursor
	snapshot.State.MembershipEpoch++
	snapshot.Stores[storeID] = current
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
		change, _ := PendingPeerChangeFromEvent(event)
		if current, ok := snapshot.Descriptors[event.PeerChange.RegionID]; ok {
			change.Base = current.Clone()
		}
		snapshot.State.ClusterEpoch++
		snapshot.Descriptors[event.PeerChange.RegionID] = change.Target.Clone()
		snapshot.PendingPeerChanges[event.PeerChange.RegionID] = change
		return true
	case rootevent.KindPeerAdditionCancelled, rootevent.KindPeerRemovalCancelled:
		pending, exists := snapshot.PendingPeerChanges[event.PeerChange.RegionID]
		if !exists || !PendingPeerChangeMatchesEvent(pending, event) {
			return true
		}
		snapshot.State.ClusterEpoch++
		if pending.Base.RegionID != 0 {
			snapshot.Descriptors[event.PeerChange.RegionID] = pending.Base.Clone()
		} else {
			delete(snapshot.Descriptors, event.PeerChange.RegionID)
		}
		delete(snapshot.PendingPeerChanges, event.PeerChange.RegionID)
		return true
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		change, _ := PendingPeerChangeFromEvent(event)
		current, hasCurrent := snapshot.Descriptors[event.PeerChange.RegionID]
		completion := ObservePeerChangeCompletion(snapshot.PendingPeerChanges, current, hasCurrent, event)
		if completion.Open() {
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
	case rootevent.KindRegionSplitPlanned:
		key, change, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		if current, ok := snapshot.Descriptors[event.RangeSplit.ParentRegionID]; ok {
			change.BaseParent = current.Clone()
		}
		snapshot.State.ClusterEpoch++
		delete(snapshot.Descriptors, event.RangeSplit.ParentRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeSplit.ParentRegionID)
		snapshot.Descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
		snapshot.Descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
		snapshot.PendingRangeChanges[key] = change
		return true
	case rootevent.KindRegionSplitCommitted:
		key, _, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		completion := ObserveRangeChangeCompletion(snapshot.PendingRangeChanges, snapshot.Descriptors, event)
		if completion.NeedsEpochAdvance(false) {
			snapshot.State.ClusterEpoch++
		}
		delete(snapshot.Descriptors, event.RangeSplit.ParentRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeSplit.ParentRegionID)
		snapshot.Descriptors[event.RangeSplit.Left.RegionID] = event.RangeSplit.Left.Clone()
		snapshot.Descriptors[event.RangeSplit.Right.RegionID] = event.RangeSplit.Right.Clone()
		delete(snapshot.PendingRangeChanges, key)
		return true
	case rootevent.KindRegionSplitCancelled:
		key, _, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		pending, exists := snapshot.PendingRangeChanges[key]
		if !exists || !PendingRangeChangeMatchesEvent(pending, event) {
			return true
		}
		snapshot.State.ClusterEpoch++
		delete(snapshot.Descriptors, event.RangeSplit.Left.RegionID)
		delete(snapshot.Descriptors, event.RangeSplit.Right.RegionID)
		if pending.BaseParent.RegionID != 0 {
			snapshot.Descriptors[pending.BaseParent.RegionID] = pending.BaseParent.Clone()
		}
		delete(snapshot.PendingRangeChanges, key)
		return true
	case rootevent.KindRegionMergePlanned:
		key, change, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		if current, ok := snapshot.Descriptors[event.RangeMerge.LeftRegionID]; ok {
			change.BaseLeft = current.Clone()
		}
		if current, ok := snapshot.Descriptors[event.RangeMerge.RightRegionID]; ok {
			change.BaseRight = current.Clone()
		}
		snapshot.State.ClusterEpoch++
		delete(snapshot.Descriptors, event.RangeMerge.LeftRegionID)
		delete(snapshot.Descriptors, event.RangeMerge.RightRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.LeftRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.RightRegionID)
		delete(snapshot.PendingRangeChanges, event.RangeMerge.LeftRegionID)
		delete(snapshot.PendingRangeChanges, event.RangeMerge.RightRegionID)
		snapshot.Descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
		snapshot.PendingRangeChanges[key] = change
		return true
	case rootevent.KindRegionMerged:
		key, _, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		completion := ObserveRangeChangeCompletion(snapshot.PendingRangeChanges, snapshot.Descriptors, event)
		if completion.NeedsEpochAdvance(false) {
			snapshot.State.ClusterEpoch++
		}
		delete(snapshot.Descriptors, event.RangeMerge.LeftRegionID)
		delete(snapshot.Descriptors, event.RangeMerge.RightRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.LeftRegionID)
		delete(snapshot.PendingPeerChanges, event.RangeMerge.RightRegionID)
		delete(snapshot.PendingRangeChanges, event.RangeMerge.LeftRegionID)
		delete(snapshot.PendingRangeChanges, event.RangeMerge.RightRegionID)
		snapshot.Descriptors[event.RangeMerge.Merged.RegionID] = event.RangeMerge.Merged.Clone()
		delete(snapshot.PendingRangeChanges, key)
		return true
	case rootevent.KindRegionMergeCancelled:
		key, _, ok := PendingRangeChangeFromEvent(event)
		if !ok {
			return false
		}
		pending, exists := snapshot.PendingRangeChanges[key]
		if !exists || !PendingRangeChangeMatchesEvent(pending, event) {
			return true
		}
		snapshot.State.ClusterEpoch++
		delete(snapshot.Descriptors, event.RangeMerge.Merged.RegionID)
		if pending.BaseLeft.RegionID != 0 {
			snapshot.Descriptors[pending.BaseLeft.RegionID] = pending.BaseLeft.Clone()
		}
		if pending.BaseRight.RegionID != 0 {
			snapshot.Descriptors[pending.BaseRight.RegionID] = pending.BaseRight.Clone()
		}
		delete(snapshot.PendingRangeChanges, key)
		return true
	default:
		return false
	}
}
