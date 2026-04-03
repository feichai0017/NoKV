package local

import (
	"sort"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

func applyEvent(state *rootpkg.State, cursor rootpkg.Cursor, event rootpkg.Event) {
	if state == nil {
		return
	}
	switch event.Kind {
	case rootpkg.EventKindStoreJoined, rootpkg.EventKindStoreLeft, rootpkg.EventKindStoreMarkedDraining:
		state.MembershipEpoch++
	case rootpkg.EventKindRegionBootstrap,
		rootpkg.EventKindRegionDescriptorPublished,
		rootpkg.EventKindRegionTombstoned,
		rootpkg.EventKindRegionSplitRequested,
		rootpkg.EventKindRegionSplitCommitted,
		rootpkg.EventKindRegionMerged,
		rootpkg.EventKindPeerAdded,
		rootpkg.EventKindPeerRemoved:
		state.ClusterEpoch++
	case rootpkg.EventKindPlacementPolicyChanged:
		if event.PlacementPolicy != nil && event.PlacementPolicy.Version > state.PolicyVersion {
			state.PolicyVersion = event.PlacementPolicy.Version
		} else {
			state.PolicyVersion++
		}
	}
	state.LastCommitted = cursor
}

func nextCursor(prev rootpkg.Cursor) rootpkg.Cursor {
	term := prev.Term
	if term == 0 {
		term = 1
	}
	return rootpkg.Cursor{Term: term, Index: prev.Index + 1}
}

func snapshotEvents(descs map[uint64]descriptor.Descriptor) []rootpkg.Event {
	if len(descs) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(descs))
	for id := range descs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	events := make([]rootpkg.Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, rootpkg.RegionDescriptorPublished(descs[id]))
	}
	return events
}

func cloneDescriptors(in map[uint64]descriptor.Descriptor) map[uint64]descriptor.Descriptor {
	if len(in) == 0 {
		return make(map[uint64]descriptor.Descriptor)
	}
	out := make(map[uint64]descriptor.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

func after(a, b rootpkg.Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
}

func previousCursor(in rootpkg.Cursor) rootpkg.Cursor {
	if in.Index <= 1 {
		return rootpkg.Cursor{}
	}
	return rootpkg.Cursor{Term: in.Term, Index: in.Index - 1}
}

func retainedFloor(records []record, fallback rootpkg.Cursor) rootpkg.Cursor {
	if len(records) == 0 {
		return fallback
	}
	return previousCursor(records[0].cursor)
}

func cloneState(in rootpkg.State) rootpkg.State { return in }

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
		if in.RangeSplit.SplitKey != nil {
			cp.SplitKey = append([]byte(nil), in.RangeSplit.SplitKey...)
		}
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
