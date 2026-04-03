package root

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
