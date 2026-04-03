package local

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

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
	return rootpkg.CursorAfter(a, b)
}

func previousCursor(in rootpkg.Cursor) rootpkg.Cursor {
	if in.Index <= 1 {
		return rootpkg.Cursor{}
	}
	return rootpkg.Cursor{Term: in.Term, Index: in.Index - 1}
}

func retainedFloor(records []rootpkg.CommittedEvent, fallback rootpkg.Cursor) rootpkg.Cursor {
	if len(records) == 0 {
		return fallback
	}
	return previousCursor(records[0].Cursor)
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
