package root

import (
	"sort"

	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// ApplyEventToState applies one rooted metadata event into compact root state.
func ApplyEventToState(state *State, cursor Cursor, event Event) {
	if state == nil {
		return
	}
	switch event.Kind {
	case EventKindStoreJoined, EventKindStoreLeft, EventKindStoreMarkedDraining:
		state.MembershipEpoch++
	case EventKindRegionBootstrap,
		EventKindRegionDescriptorPublished,
		EventKindRegionTombstoned,
		EventKindRegionSplitRequested,
		EventKindRegionSplitCommitted,
		EventKindRegionMerged,
		EventKindPeerAdded,
		EventKindPeerRemoved:
		state.ClusterEpoch++
	case EventKindPlacementPolicyChanged:
		if event.PlacementPolicy != nil && event.PlacementPolicy.Version > state.PolicyVersion {
			state.PolicyVersion = event.PlacementPolicy.Version
		} else {
			state.PolicyVersion++
		}
	}
	state.LastCommitted = cursor
}

// NextCursor returns the next ordered root cursor.
func NextCursor(prev Cursor) Cursor {
	term := prev.Term
	if term == 0 {
		term = 1
	}
	return Cursor{Term: term, Index: prev.Index + 1}
}

// CursorAfter reports whether a is ordered strictly after b.
func CursorAfter(a, b Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
}

// SnapshotDescriptorEvents materializes descriptor truth into a stable event
// sequence for bootstrap/recovery callers.
func SnapshotDescriptorEvents(descs map[uint64]descriptor.Descriptor) []Event {
	if len(descs) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(descs))
	for id := range descs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	events := make([]Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, RegionDescriptorPublished(descs[id]))
	}
	return events
}
