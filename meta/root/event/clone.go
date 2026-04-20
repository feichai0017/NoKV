package event

import rootproto "github.com/feichai0017/NoKV/meta/root/protocol"

// CloneEvent returns a detached rooted metadata event copy.
func CloneEvent(in Event) Event {
	out := in
	if in.StoreMembership != nil {
		cp := *in.StoreMembership
		out.StoreMembership = &cp
	}
	if in.AllocatorFence != nil {
		cp := *in.AllocatorFence
		out.AllocatorFence = &cp
	}
	if in.CoordinatorLease != nil {
		cp := *in.CoordinatorLease
		cp.Frontiers = rootproto.CloneDutyFrontiers(in.CoordinatorLease.Frontiers)
		out.CoordinatorLease = &cp
	}
	if in.CoordinatorSeal != nil {
		cp := *in.CoordinatorSeal
		cp.Frontiers = rootproto.CloneDutyFrontiers(in.CoordinatorSeal.Frontiers)
		out.CoordinatorSeal = &cp
	}
	if in.CoordinatorClosure != nil {
		cp := *in.CoordinatorClosure
		out.CoordinatorClosure = &cp
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
		cp.BaseParent = in.RangeSplit.BaseParent.Clone()
		out.RangeSplit = &cp
	}
	if in.RangeMerge != nil {
		cp := *in.RangeMerge
		cp.Merged = in.RangeMerge.Merged.Clone()
		cp.BaseLeft = in.RangeMerge.BaseLeft.Clone()
		cp.BaseRight = in.RangeMerge.BaseRight.Clone()
		out.RangeMerge = &cp
	}
	if in.PeerChange != nil {
		cp := *in.PeerChange
		cp.Region = in.PeerChange.Region.Clone()
		cp.Base = in.PeerChange.Base.Clone()
		out.PeerChange = &cp
	}
	return out
}
