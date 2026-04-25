package event

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
	if in.Tenure != nil {
		cp := *in.Tenure
		cp.Frontiers = in.Tenure.Frontiers
		out.Tenure = &cp
	}
	if in.Legacy != nil {
		cp := *in.Legacy
		cp.Frontiers = in.Legacy.Frontiers
		out.Legacy = &cp
	}
	if in.Handover != nil {
		cp := *in.Handover
		out.Handover = &cp
	}
	if in.SnapshotEpoch != nil {
		cp := *in.SnapshotEpoch
		out.SnapshotEpoch = &cp
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
