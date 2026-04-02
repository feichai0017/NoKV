package rootraft

import (
	"sync"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

type appliedRecord struct {
	cursor rootpkg.Cursor
	event  rootpkg.Event
}

// StateMachine materializes the compact metadata-root truth from committed raft
// commands.
type StateMachine struct {
	mu          sync.RWMutex
	state       rootpkg.State
	descriptors map[uint64]descriptor.Descriptor
	records     []appliedRecord
}

func NewStateMachine(checkpoint Checkpoint) *StateMachine {
	cp := checkpoint.Clone()
	if cp.Descriptors == nil {
		cp.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	return &StateMachine{state: cp.State, descriptors: cp.Descriptors}
}

func (sm *StateMachine) Current() rootpkg.State {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state
}

func (sm *StateMachine) ReadSince(cursor rootpkg.Cursor) ([]rootpkg.Event, rootpkg.Cursor) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make([]rootpkg.Event, 0, len(sm.records))
	for _, rec := range sm.records {
		if afterCursor(rec.cursor, cursor) {
			out = append(out, cloneEvent(rec.event))
		}
	}
	return out, sm.state.LastCommitted
}

func (sm *StateMachine) Snapshot() Checkpoint {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := Checkpoint{State: sm.state}
	if len(sm.descriptors) > 0 {
		out.Descriptors = make(map[uint64]descriptor.Descriptor, len(sm.descriptors))
		for id, desc := range sm.descriptors {
			out.Descriptors[id] = desc.Clone()
		}
	}
	return out
}

func (sm *StateMachine) ApplyBarrier(cursor rootpkg.Cursor) rootpkg.CommitInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.state.LastCommitted = cursor
	return rootpkg.CommitInfo{Cursor: cursor, State: sm.state}
}

func (sm *StateMachine) ApplyCommand(cursor rootpkg.Cursor, cmd command) rootpkg.CommitInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch cmd.kind {
	case commandKindFence:
		switch cmd.fence.kind {
		case rootpkg.AllocatorKindID:
			if sm.state.IDFence < cmd.fence.min {
				sm.state.IDFence = cmd.fence.min
			}
		case rootpkg.AllocatorKindTSO:
			if sm.state.TSOFence < cmd.fence.min {
				sm.state.TSOFence = cmd.fence.min
			}
		}
		sm.state.LastCommitted = cursor
	case commandKindEvent:
		sm.applyEventLocked(cursor, cmd.event)
	default:
		sm.state.LastCommitted = cursor
	}
	return rootpkg.CommitInfo{Cursor: cursor, State: sm.state}
}

func (sm *StateMachine) applyEventLocked(cursor rootpkg.Cursor, event rootpkg.Event) {
	switch event.Kind {
	case rootpkg.EventKindStoreJoined, rootpkg.EventKindStoreLeft, rootpkg.EventKindStoreMarkedDraining:
		sm.state.MembershipEpoch++
	case rootpkg.EventKindRegionBootstrap, rootpkg.EventKindRegionDescriptorPublished, rootpkg.EventKindRegionTombstoned, rootpkg.EventKindRegionSplitRequested, rootpkg.EventKindRegionSplitCommitted, rootpkg.EventKindRegionMerged, rootpkg.EventKindPeerAdded, rootpkg.EventKindPeerRemoved:
		sm.state.ClusterEpoch++
	case rootpkg.EventKindPlacementPolicyChanged:
		if event.PlacementPolicy != nil && event.PlacementPolicy.Version > sm.state.PolicyVersion {
			sm.state.PolicyVersion = event.PlacementPolicy.Version
		} else {
			sm.state.PolicyVersion++
		}
	}
	switch {
	case event.RegionDescriptor != nil:
		sm.descriptors[event.RegionDescriptor.Descriptor.RegionID] = event.RegionDescriptor.Descriptor.Clone()
	case event.RegionRemoval != nil:
		delete(sm.descriptors, event.RegionRemoval.RegionID)
	case event.RangeSplit != nil && event.Kind == rootpkg.EventKindRegionSplitCommitted:
		left := event.RangeSplit.Left.Clone()
		right := event.RangeSplit.Right.Clone()
		sm.descriptors[left.RegionID] = left
		sm.descriptors[right.RegionID] = right
		if parent := event.RangeSplit.ParentRegionID; parent != 0 && parent != left.RegionID && parent != right.RegionID {
			delete(sm.descriptors, parent)
		}
	case event.RangeMerge != nil:
		delete(sm.descriptors, event.RangeMerge.LeftRegionID)
		delete(sm.descriptors, event.RangeMerge.RightRegionID)
		merged := event.RangeMerge.Merged.Clone()
		sm.descriptors[merged.RegionID] = merged
	case event.PeerChange != nil:
		sm.descriptors[event.PeerChange.Region.RegionID] = event.PeerChange.Region.Clone()
	}
	sm.records = append(sm.records, appliedRecord{cursor: cursor, event: cloneEvent(event)})
	sm.state.LastCommitted = cursor
}

func cloneEvent(in rootpkg.Event) rootpkg.Event {
	out := in
	if in.StoreMembership != nil {
		cp := *in.StoreMembership
		out.StoreMembership = &cp
	}
	if in.RegionDescriptor != nil {
		cp := *in.RegionDescriptor
		cp.Descriptor = in.RegionDescriptor.Descriptor.Clone()
		out.RegionDescriptor = &cp
	}
	if in.RegionRemoval != nil {
		cp := *in.RegionRemoval
		out.RegionRemoval = &cp
	}
	if in.RangeSplit != nil {
		cp := *in.RangeSplit
		cp.SplitKey = append([]byte(nil), in.RangeSplit.SplitKey...)
		cp.Left = in.RangeSplit.Left.Clone()
		cp.Right = in.RangeSplit.Right.Clone()
		out.RangeSplit = &cp
	}
	if in.RangeMerge != nil {
		cp := *in.RangeMerge
		cp.Merged = in.RangeMerge.Merged.Clone()
		out.RangeMerge = &cp
	}
	if in.PeerChange != nil {
		cp := *in.PeerChange
		cp.Region = in.PeerChange.Region.Clone()
		out.PeerChange = &cp
	}
	if in.LeaderTransfer != nil {
		cp := *in.LeaderTransfer
		out.LeaderTransfer = &cp
	}
	if in.PlacementPolicy != nil {
		cp := *in.PlacementPolicy
		out.PlacementPolicy = &cp
	}
	return out
}

func afterCursor(a, b rootpkg.Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
}
