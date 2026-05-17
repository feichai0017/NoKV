// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/meta/topology"
	"github.com/stretchr/testify/require"
)

func TestRegionSplitCommittedClonesSplitKey(t *testing.T) {
	key := []byte("m")
	left := testDescriptor(1, []byte("a"), []byte("m"))
	right := testDescriptor(2, []byte("m"), []byte("z"))

	event := rootevent.RegionSplitCommitted(1, key, left, right)
	key[0] = 'x'

	require.Equal(t, rootevent.KindRegionSplitCommitted, event.Kind)
	require.NotNil(t, event.RangeSplit)
	require.Equal(t, []byte("m"), event.RangeSplit.SplitKey)
}

func TestRegionSplitPlannedClonesSplitKey(t *testing.T) {
	key := []byte("m")
	left := testDescriptor(1, []byte("a"), []byte("m"))
	right := testDescriptor(2, []byte("m"), []byte("z"))

	event := rootevent.RegionSplitPlanned(1, key, left, right)
	key[0] = 'x'

	require.Equal(t, rootevent.KindRegionSplitPlanned, event.Kind)
	require.NotNil(t, event.RangeSplit)
	require.Equal(t, []byte("m"), event.RangeSplit.SplitKey)
}

func TestCloneEventDetachesPayload(t *testing.T) {
	in := rootevent.RegionDescriptorPublished(testDescriptor(9, []byte("a"), []byte("z")))
	cloned := rootevent.CloneEvent(in)

	in.RegionDescriptor.Descriptor.StartKey[0] = 'x'
	require.Equal(t, byte('a'), cloned.RegionDescriptor.Descriptor.StartKey[0])
}

func TestGrantLifecycleEventsDetachPayload(t *testing.T) {
	grant := rootproto.AuthorityGrant{
		GrantID:  "c1/1",
		HolderID: "c1",
		Era:      1,
		Duties: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10),
		},
		PredecessorRetirements: []rootproto.GrantRetirement{
			{GrantID: "old/1", Era: 1, Mode: rootproto.GrantRetirementExpiredBound},
		},
	}
	issued := rootevent.GrantIssued(grant)
	issuedClone := rootevent.CloneEvent(issued)
	issued.Grant.Duties[0].Bound.MonotoneUpper = 99
	require.Equal(t, rootevent.KindGrantIssued, issuedClone.Kind)
	require.Equal(t, uint64(10), issuedClone.Grant.Duties[0].Bound.MonotoneUpper)

	retirement := rootproto.GrantRetirement{
		GrantID:  "c1/1",
		HolderID: "c1",
		Era:      1,
		Bounds: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 20),
		},
	}
	sealed := rootevent.GrantSealed(retirement)
	sealedClone := rootevent.CloneEvent(sealed)
	sealed.GrantRetirement.Bounds[0].Bound.MonotoneUpper = 200
	require.Equal(t, rootevent.KindGrantSealed, sealedClone.Kind)
	require.Equal(t, rootproto.GrantRetirementSealedExact, sealedClone.GrantRetirement.Mode)
	require.Equal(t, uint64(20), sealedClone.GrantRetirement.Bounds[0].Bound.MonotoneUpper)

	inherited := rootevent.GrantInherited(rootproto.GrantInheritance{PredecessorGrantID: "c1/1", SuccessorGrantID: "c2/2"})
	inheritedClone := rootevent.CloneEvent(inherited)
	inherited.GrantInheritance.SuccessorGrantID = "mutated"
	require.Equal(t, rootevent.KindGrantInherited, inheritedClone.Kind)
	require.Equal(t, "c2/2", inheritedClone.GrantInheritance.SuccessorGrantID)

	visibleGrant := rootproto.VisibleAuthorityGrant{
		GrantID:  "visible-1",
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{1},
		},
		ExpiresUnixNano: 1_000,
	}
	visibleAuthorityIssued := rootevent.VisibleAuthorityGranted(visibleGrant)
	visibleAuthorityIssued.VisibleGrant.Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, visibleGrant.Scope.Buckets)
	visibleAuthorityRetired := rootevent.VisibleAuthorityRetired(visibleGrant)
	require.Equal(t, rootevent.KindVisibleAuthorityRetired, visibleAuthorityRetired.Kind)
	require.Equal(t, visibleGrant.GrantID, visibleAuthorityRetired.VisibleGrant.GrantID)

	var root [32]byte
	var digest [32]byte
	root[0] = 1
	digest[0] = 2
	visibleSeal := rootproto.VisibleAuthoritySeal{
		GrantID:              visibleGrant.GrantID,
		EpochID:              visibleGrant.EpochID,
		HolderID:             visibleGrant.HolderID,
		Scope:                visibleGrant.Scope,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SealedUnixNano:       10,
		InstallRegionID:      11,
		InstallTerm:          12,
		InstallIndex:         13,
		InstallVersion:       14,
	}
	visibleSealed := rootevent.VisibleAuthoritySealed(visibleSeal)
	visibleSealedClone := rootevent.CloneEvent(visibleSealed)
	visibleSealed.VisibleSeal.Scope.Buckets[0] = 10
	require.Equal(t, rootevent.KindVisibleAuthoritySealed, visibleSealedClone.Kind)
	require.Equal(t, []uint16{1}, visibleSealedClone.VisibleSeal.Scope.Buckets)
}

func TestMembershipAndAllocatorConstructors(t *testing.T) {
	joined := rootevent.StoreJoined(7)
	retired := rootevent.StoreRetired(7)
	idFence := rootevent.IDAllocatorFenced(11)
	tsoFence := rootevent.TSOAllocatorFenced(29)
	ref := testEventSnapshotEvidenceRef(7, 0x60)
	refs := []rootproto.SnapshotEvidenceRef{ref}
	snapshot := rootevent.SnapshotEpochPublishedWithRuntimeEvidence("vol", 1, 42, 99, refs)
	refs[0].EvidenceRoot[0] = 0xff
	retiredSnapshot := rootevent.SnapshotEpochRetired("vol", 1, 42, 99)
	mount := rootevent.MountRegistered("vol", 1, 1, 1)
	retiredMount := rootevent.MountRetired("vol")
	declaredSubtree := rootevent.SubtreeAuthorityDeclared("vol", 1, "vol", 0, 10)
	startedSubtree := rootevent.SubtreeHandoffStarted("vol", 1, 11)
	completedSubtree := rootevent.SubtreeHandoffCompleted("vol", 1, 12)
	quota := rootevent.QuotaFenceUpdated("vol", 1, 4096, 12, 2, 99)

	require.Equal(t, rootevent.KindStoreJoined, joined.Kind)
	require.Equal(t, uint64(7), joined.StoreMembership.StoreID)

	require.Equal(t, rootevent.KindStoreRetired, retired.Kind)
	require.Equal(t, uint64(7), retired.StoreMembership.StoreID)

	require.Equal(t, rootevent.KindIDAllocatorFenced, idFence.Kind)
	require.Equal(t, uint64(11), idFence.AllocatorFence.Minimum)

	require.Equal(t, rootevent.KindTSOAllocatorFenced, tsoFence.Kind)
	require.Equal(t, uint64(29), tsoFence.AllocatorFence.Minimum)

	require.Equal(t, rootevent.KindSnapshotEpochPublished, snapshot.Kind)
	require.Equal(t, rootevent.SnapshotEpochID("vol", 42, 99), snapshot.SnapshotEpoch.SnapshotID)
	require.Equal(t, uint64(1), snapshot.SnapshotEpoch.MountKeyID)
	require.Equal(t, uint64(42), snapshot.SnapshotEpoch.RootInode)
	require.Equal(t, uint64(99), snapshot.SnapshotEpoch.ReadVersion)
	require.Equal(t, []rootproto.SnapshotEvidenceRef{ref}, snapshot.SnapshotEpoch.RuntimeEvidence)
	require.Equal(t, rootevent.KindSnapshotEpochRetired, retiredSnapshot.Kind)
	require.Equal(t, snapshot.SnapshotEpoch.SnapshotID, retiredSnapshot.SnapshotEpoch.SnapshotID)
	require.Equal(t, uint64(1), retiredSnapshot.SnapshotEpoch.MountKeyID)
	require.Equal(t, rootevent.KindMountRegistered, mount.Kind)
	require.Equal(t, "vol", mount.Mount.MountID)
	require.Equal(t, uint64(1), mount.Mount.MountKeyID)
	require.Equal(t, uint64(1), mount.Mount.RootInode)
	require.Equal(t, uint32(1), mount.Mount.SchemaVersion)
	require.Equal(t, rootevent.KindMountRetired, retiredMount.Kind)
	require.Equal(t, "vol", retiredMount.Mount.MountID)

	require.Equal(t, rootevent.KindSubtreeAuthorityDeclared, declaredSubtree.Kind)
	require.Equal(t, "vol", declaredSubtree.SubtreeAuthority.Mount)
	require.Equal(t, uint64(1), declaredSubtree.SubtreeAuthority.RootInode)
	require.Equal(t, "vol", declaredSubtree.SubtreeAuthority.AuthorityID)
	require.Equal(t, uint64(10), declaredSubtree.SubtreeAuthority.Frontier)
	require.Equal(t, rootevent.KindSubtreeHandoffStarted, startedSubtree.Kind)
	require.Equal(t, uint64(11), startedSubtree.SubtreeAuthority.Frontier)
	require.Equal(t, rootevent.KindSubtreeHandoffCompleted, completedSubtree.Kind)
	require.Equal(t, uint64(12), completedSubtree.SubtreeAuthority.InheritedFrontier)
	require.Equal(t, rootevent.KindQuotaFenceUpdated, quota.Kind)
	require.Equal(t, rootevent.QuotaFenceID("vol", 1), quota.QuotaFence.SubjectID)
	require.Equal(t, uint64(4096), quota.QuotaFence.LimitBytes)
	require.Equal(t, uint64(12), quota.QuotaFence.LimitInodes)
	require.Equal(t, uint64(2), quota.QuotaFence.Era)
	require.Equal(t, uint64(99), quota.QuotaFence.Frontier)
}

func testEventSnapshotEvidenceRef(epoch uint64, seed byte) rootproto.SnapshotEvidenceRef {
	var root [32]byte
	var digest [32]byte
	root[0] = seed
	digest[0] = seed + 1
	return rootproto.SnapshotEvidenceRef{EpochID: epoch, EvidenceRoot: root, PayloadDigest: digest}
}

func TestRegionLifecycleConstructors(t *testing.T) {
	parent := testDescriptor(1, []byte("a"), []byte("z"))
	left := testDescriptor(1, []byte("a"), []byte("m"))
	right := testDescriptor(2, []byte("m"), []byte("z"))

	bootstrapped := rootevent.RegionBootstrapped(parent)
	tombstoned := rootevent.RegionTombstoned(9)
	cancelledSplit := rootevent.RegionSplitCancelled(1, []byte("m"), left, right, parent)
	plannedMerge := rootevent.RegionMergePlanned(1, 2, parent)
	merged := rootevent.RegionMerged(1, 2, parent)
	cancelledMerge := rootevent.RegionMergeCancelled(1, 2, parent, left, right)

	require.Equal(t, rootevent.KindRegionBootstrap, bootstrapped.Kind)
	require.Equal(t, parent.RegionID, bootstrapped.RegionDescriptor.Descriptor.RegionID)

	require.Equal(t, rootevent.KindRegionTombstoned, tombstoned.Kind)
	require.Equal(t, uint64(9), tombstoned.RegionRemoval.RegionID)

	require.Equal(t, rootevent.KindRegionSplitCancelled, cancelledSplit.Kind)
	require.Equal(t, []byte("m"), cancelledSplit.RangeSplit.SplitKey)
	require.Equal(t, parent.RegionID, cancelledSplit.RangeSplit.BaseParent.RegionID)

	require.Equal(t, rootevent.KindRegionMergePlanned, plannedMerge.Kind)
	require.Equal(t, uint64(1), plannedMerge.RangeMerge.LeftRegionID)
	require.Equal(t, uint64(2), plannedMerge.RangeMerge.RightRegionID)

	require.Equal(t, rootevent.KindRegionMerged, merged.Kind)
	require.Equal(t, parent.RegionID, merged.RangeMerge.Merged.RegionID)

	require.Equal(t, rootevent.KindRegionMergeCancelled, cancelledMerge.Kind)
	require.Equal(t, left.RegionID, cancelledMerge.RangeMerge.BaseLeft.RegionID)
	require.Equal(t, right.RegionID, cancelledMerge.RangeMerge.BaseRight.RegionID)
}

func TestPeerChangeConstructors(t *testing.T) {
	region := testDescriptor(11, []byte("a"), []byte("z"))
	base := testDescriptor(11, []byte("a"), []byte("z"))

	added := rootevent.PeerAdded(11, 2, 201, region)
	addPlanned := rootevent.PeerAdditionPlanned(11, 2, 201, region)
	addCancelled := rootevent.PeerAdditionCancelled(11, 2, 201, region, base)
	removePlanned := rootevent.PeerRemovalPlanned(11, 2, 201, region)
	removeCancelled := rootevent.PeerRemovalCancelled(11, 2, 201, region, base)
	removed := rootevent.PeerRemoved(11, 2, 201, region)

	require.Equal(t, rootevent.KindPeerAdded, added.Kind)
	require.Equal(t, uint64(11), added.PeerChange.RegionID)
	require.Equal(t, uint64(2), added.PeerChange.StoreID)
	require.Equal(t, uint64(201), added.PeerChange.PeerID)

	require.Equal(t, rootevent.KindPeerAdditionPlanned, addPlanned.Kind)
	require.Equal(t, region.RegionID, addPlanned.PeerChange.Region.RegionID)

	require.Equal(t, rootevent.KindPeerAdditionCancelled, addCancelled.Kind)
	require.Equal(t, base.RegionID, addCancelled.PeerChange.Base.RegionID)

	require.Equal(t, rootevent.KindPeerRemovalPlanned, removePlanned.Kind)
	require.Equal(t, region.RegionID, removePlanned.PeerChange.Region.RegionID)

	require.Equal(t, rootevent.KindPeerRemovalCancelled, removeCancelled.Kind)
	require.Equal(t, base.RegionID, removeCancelled.PeerChange.Base.RegionID)

	require.Equal(t, rootevent.KindPeerRemoved, removed.Kind)
	require.Equal(t, region.RegionID, removed.PeerChange.Region.RegionID)
}

func testDescriptor(id uint64, start, end []byte) topology.Descriptor {
	desc := topology.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: id*10 + 1}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
