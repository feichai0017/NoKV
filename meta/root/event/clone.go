// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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
	if in.Grant != nil {
		cp := *in.Grant
		cp.Duties = append([]rootproto.DutyGrant(nil), in.Grant.Duties...)
		cp.PredecessorRetirements = append([]rootproto.GrantRetirement(nil), in.Grant.PredecessorRetirements...)
		out.Grant = &cp
	}
	if in.GrantRetirement != nil {
		cp := *in.GrantRetirement
		cp.Bounds = append([]rootproto.DutyGrant(nil), in.GrantRetirement.Bounds...)
		out.GrantRetirement = &cp
	}
	if in.GrantInheritance != nil {
		cp := *in.GrantInheritance
		out.GrantInheritance = &cp
	}
	if in.PerasGrant != nil {
		cp := rootproto.ClonePerasAuthorityGrant(*in.PerasGrant)
		out.PerasGrant = &cp
	}
	if in.PerasSeal != nil {
		cp := rootproto.ClonePerasAuthoritySeal(*in.PerasSeal)
		out.PerasSeal = &cp
	}
	if in.SnapshotEpoch != nil {
		cp := *in.SnapshotEpoch
		cp.RuntimeEvidence = rootproto.CloneSnapshotEvidenceRefs(in.SnapshotEpoch.RuntimeEvidence)
		out.SnapshotEpoch = &cp
	}
	if in.Mount != nil {
		cp := *in.Mount
		out.Mount = &cp
	}
	if in.SubtreeAuthority != nil {
		cp := *in.SubtreeAuthority
		out.SubtreeAuthority = &cp
	}
	if in.QuotaFence != nil {
		cp := *in.QuotaFence
		out.QuotaFence = &cp
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
