package event_test

import (
	"testing"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
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

func TestCoordinatorLeaseEvent(t *testing.T) {
	frontiers := controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 0)
	event := rootevent.CoordinatorLeaseGranted("c1", 1_000, 1, 7, "pred", frontiers)
	cloned := rootevent.CloneEvent(event)

	event.CoordinatorLease.HolderID = "c2"
	require.Equal(t, rootevent.KindCoordinatorLease, cloned.Kind)
	require.Equal(t, "c1", cloned.CoordinatorLease.HolderID)
	require.Equal(t, int64(1_000), cloned.CoordinatorLease.ExpiresUnixNano)
	require.Equal(t, uint64(1), cloned.CoordinatorLease.CertGeneration)
	require.Equal(t, uint32(7), cloned.CoordinatorLease.DutyMask)
	require.Equal(t, frontiers, cloned.CoordinatorLease.Frontiers)
	require.Equal(t, "pred", cloned.CoordinatorLease.PredecessorDigest)
}

func TestCoordinatorClosureConfirmedEvent(t *testing.T) {
	event := rootevent.CoordinatorClosureConfirmed("c1", 7, 8, "seal-digest")
	cloned := rootevent.CloneEvent(event)

	event.CoordinatorClosure.HolderID = "c2"
	require.Equal(t, rootevent.KindCoordinatorClosure, cloned.Kind)
	require.Equal(t, "c1", cloned.CoordinatorClosure.HolderID)
	require.Equal(t, uint64(7), cloned.CoordinatorClosure.SealGeneration)
	require.Equal(t, uint64(8), cloned.CoordinatorClosure.SuccessorGeneration)
	require.Equal(t, "seal-digest", cloned.CoordinatorClosure.SealDigest)
	require.Equal(t, rootevent.CoordinatorClosureStageConfirmed, cloned.CoordinatorClosure.Stage)
}

func TestCoordinatorClosureClosedEvent(t *testing.T) {
	event := rootevent.CoordinatorClosureClosed("c1", 7, 8, "seal-digest")
	cloned := rootevent.CloneEvent(event)

	event.CoordinatorClosure.HolderID = "c2"
	require.Equal(t, rootevent.KindCoordinatorClosure, cloned.Kind)
	require.Equal(t, "c1", cloned.CoordinatorClosure.HolderID)
	require.Equal(t, uint64(7), cloned.CoordinatorClosure.SealGeneration)
	require.Equal(t, uint64(8), cloned.CoordinatorClosure.SuccessorGeneration)
	require.Equal(t, "seal-digest", cloned.CoordinatorClosure.SealDigest)
	require.Equal(t, rootevent.CoordinatorClosureStageClosed, cloned.CoordinatorClosure.Stage)
}

func TestCoordinatorClosureReattachedEvent(t *testing.T) {
	event := rootevent.CoordinatorClosureReattached("c1", 7, 8, "seal-digest")
	cloned := rootevent.CloneEvent(event)

	event.CoordinatorClosure.HolderID = "c2"
	require.Equal(t, rootevent.KindCoordinatorClosure, cloned.Kind)
	require.Equal(t, "c1", cloned.CoordinatorClosure.HolderID)
	require.Equal(t, uint64(7), cloned.CoordinatorClosure.SealGeneration)
	require.Equal(t, uint64(8), cloned.CoordinatorClosure.SuccessorGeneration)
	require.Equal(t, "seal-digest", cloned.CoordinatorClosure.SealDigest)
	require.Equal(t, rootevent.CoordinatorClosureStageReattached, cloned.CoordinatorClosure.Stage)
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
