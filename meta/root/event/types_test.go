package event_test

import (
	"testing"

	eunomia "github.com/feichai0017/NoKV/coordinator/protocol/eunomia"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
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

func TestTenureEvent(t *testing.T) {
	frontiers := eunomia.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 0)
	event := rootevent.TenureGranted("c1", 1_000, 1, 7, "pred", frontiers)
	cloned := rootevent.CloneEvent(event)

	event.Tenure.HolderID = "c2"
	require.Equal(t, rootevent.KindTenure, cloned.Kind)
	require.Equal(t, "c1", cloned.Tenure.HolderID)
	require.Equal(t, int64(1_000), cloned.Tenure.ExpiresUnixNano)
	require.Equal(t, uint64(1), cloned.Tenure.Era)
	require.Equal(t, uint32(7), cloned.Tenure.Mandate)
	require.Equal(t, frontiers, cloned.Tenure.Frontiers)
	require.Equal(t, "pred", cloned.Tenure.LineageDigest)
}

func TestHandoverConfirmedEvent(t *testing.T) {
	event := rootevent.HandoverConfirmed("c1", 7, 8, "seal-digest")
	cloned := rootevent.CloneEvent(event)

	event.Handover.HolderID = "c2"
	require.Equal(t, rootevent.KindHandover, cloned.Kind)
	require.Equal(t, "c1", cloned.Handover.HolderID)
	require.Equal(t, uint64(7), cloned.Handover.LegacyEra)
	require.Equal(t, uint64(8), cloned.Handover.SuccessorEra)
	require.Equal(t, "seal-digest", cloned.Handover.LegacyDigest)
	require.Equal(t, rootevent.HandoverStageConfirmed, cloned.Handover.Stage)
}

func TestHandoverClosedEvent(t *testing.T) {
	event := rootevent.HandoverClosed("c1", 7, 8, "seal-digest")
	cloned := rootevent.CloneEvent(event)

	event.Handover.HolderID = "c2"
	require.Equal(t, rootevent.KindHandover, cloned.Kind)
	require.Equal(t, "c1", cloned.Handover.HolderID)
	require.Equal(t, uint64(7), cloned.Handover.LegacyEra)
	require.Equal(t, uint64(8), cloned.Handover.SuccessorEra)
	require.Equal(t, "seal-digest", cloned.Handover.LegacyDigest)
	require.Equal(t, rootevent.HandoverStageClosed, cloned.Handover.Stage)
}

func TestHandoverReattachedEvent(t *testing.T) {
	event := rootevent.HandoverReattached("c1", 7, 8, "seal-digest")
	cloned := rootevent.CloneEvent(event)

	event.Handover.HolderID = "c2"
	require.Equal(t, rootevent.KindHandover, cloned.Kind)
	require.Equal(t, "c1", cloned.Handover.HolderID)
	require.Equal(t, uint64(7), cloned.Handover.LegacyEra)
	require.Equal(t, uint64(8), cloned.Handover.SuccessorEra)
	require.Equal(t, "seal-digest", cloned.Handover.LegacyDigest)
	require.Equal(t, rootevent.HandoverStageReattached, cloned.Handover.Stage)
}

func TestMembershipAndAllocatorConstructors(t *testing.T) {
	joined := rootevent.StoreJoined(7)
	retired := rootevent.StoreRetired(7)
	idFence := rootevent.IDAllocatorFenced(11)
	tsoFence := rootevent.TSOAllocatorFenced(29)
	snapshot := rootevent.SnapshotEpochPublished("vol", 42, 99)
	retiredSnapshot := rootevent.SnapshotEpochRetired("vol", 42, 99)
	mount := rootevent.MountRegistered("vol", 1, 1)
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
	require.Equal(t, uint64(42), snapshot.SnapshotEpoch.RootInode)
	require.Equal(t, uint64(99), snapshot.SnapshotEpoch.ReadVersion)
	require.Equal(t, rootevent.KindSnapshotEpochRetired, retiredSnapshot.Kind)
	require.Equal(t, snapshot.SnapshotEpoch.SnapshotID, retiredSnapshot.SnapshotEpoch.SnapshotID)
	require.Equal(t, rootevent.KindMountRegistered, mount.Kind)
	require.Equal(t, "vol", mount.Mount.MountID)
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

func TestTenureReleasedAndSealed(t *testing.T) {
	frontiers := eunomia.Frontiers(rootstate.State{IDFence: 5, TSOFence: 9}, 0)
	released := rootevent.TenureReleased("c1", 2_000, 3, 5, "digest", frontiers)
	sealed := rootevent.TenureSealed("c1", 3, 5, frontiers)

	require.Equal(t, rootevent.KindTenure, released.Kind)
	require.Equal(t, "c1", released.Tenure.HolderID)
	require.Equal(t, int64(2_000), released.Tenure.ExpiresUnixNano)
	require.Equal(t, uint64(3), released.Tenure.Era)
	require.Equal(t, uint32(5), released.Tenure.Mandate)
	require.Equal(t, "digest", released.Tenure.LineageDigest)
	require.Equal(t, frontiers, released.Tenure.Frontiers)

	require.Equal(t, rootevent.KindLegacy, sealed.Kind)
	require.Equal(t, "c1", sealed.Legacy.HolderID)
	require.Equal(t, uint64(3), sealed.Legacy.Era)
	require.Equal(t, uint32(5), sealed.Legacy.Mandate)
	require.Equal(t, frontiers, sealed.Legacy.Frontiers)
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

func testDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
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
