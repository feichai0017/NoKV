package local

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func campaignLease(store *Store, holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64, predecessorDigest string) (rootstate.CoordinatorLease, error) {
	state, err := store.ApplyCoordinatorLease(rootstate.CoordinatorLeaseCommand{
		Kind:              rootstate.CoordinatorLeaseCommandIssue,
		HolderID:          holderID,
		ExpiresUnixNano:   expiresUnixNano,
		NowUnixNano:       nowUnixNano,
		PredecessorDigest: predecessorDigest,
		HandoffFrontiers:  controlplane.Frontiers(idFence, tsoFence, descriptorRevision),
	})
	return state.Lease, err
}

func releaseLease(store *Store, holderID string, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error) {
	state, err := store.ApplyCoordinatorLease(rootstate.CoordinatorLeaseCommand{
		Kind:             rootstate.CoordinatorLeaseCommandRelease,
		HolderID:         holderID,
		NowUnixNano:      nowUnixNano,
		HandoffFrontiers: controlplane.Frontiers(idFence, tsoFence, 0),
	})
	return state.Lease, err
}

func sealLease(store *Store, holderID string, nowUnixNano int64, frontiers rootstate.CoordinatorDutyFrontiers) (rootstate.CoordinatorSeal, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandSeal,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	return state.Seal, err
}

func confirmClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandConfirm,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func closeClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandClose,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func reattachClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandReattach,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func TestStoreAppendReadAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)

	state, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, rootstate.State{}, state)

	commit, err := store.Append(
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))),
		rootevent.RegionSplitCommitted(10, []byte("m"), testDescriptor(11, []byte("a"), []byte("m")), testDescriptor(12, []byte("m"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, commit.Cursor)
	require.Equal(t, uint64(1), commit.State.MembershipEpoch)
	require.Equal(t, uint64(2), commit.State.ClusterEpoch)

	events, tail, err := store.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, commit.Cursor, tail)
	require.Equal(t, rootevent.KindStoreJoined, events[0].Kind)
	require.Equal(t, uint64(1), events[0].StoreMembership.StoreID)
	require.Equal(t, rootevent.KindRegionDescriptorPublished, events[1].Kind)
	require.Equal(t, uint64(10), events[1].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, []byte("m"), events[2].RangeSplit.SplitKey)
	require.Equal(t, uint64(11), events[2].RangeSplit.Left.RegionID)
	require.Equal(t, uint64(12), events[2].RangeSplit.Right.RegionID)

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err = reopened.Current()
	require.NoError(t, err)
	require.Equal(t, commit.State, state)
	events, tail, err = reopened.ReadSince(rootstate.Cursor{Term: 1, Index: 1})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, commit.Cursor, tail)
}

func TestStoreFenceAllocatorPersistsWithoutEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)

	fence, err := store.FenceAllocator(rootstate.AllocatorKindID, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(10), fence)
	fence, err = store.FenceAllocator(rootstate.AllocatorKindID, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(10), fence)
	fence, err = store.FenceAllocator(rootstate.AllocatorKindTSO, 22)
	require.NoError(t, err)
	require.Equal(t, uint64(22), fence)

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, uint64(10), state.IDFence)
	require.Equal(t, uint64(22), state.TSOFence)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, state.LastCommitted)
}

func TestStoreCampaignCoordinatorLease(t *testing.T) {
	store, err := Open(t.TempDir(), nil)
	require.NoError(t, err)

	lease, err := campaignLease(store, "c1", 1_000, 100, 10, 20, 30, "")
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, int64(1_000), lease.ExpiresUnixNano)

	state, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, lease, state.CoordinatorLease)
	require.Equal(t, uint64(10), state.IDFence)
	require.Equal(t, uint64(20), state.TSOFence)

	held, err := campaignLease(store, "c2", 1_500, 200, 30, 40, 30, "")
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseHeld))
	require.Equal(t, lease, held)

	lease, err = campaignLease(store, "c2", 2_000, 1_001, 30, 40, 30, "")
	require.NoError(t, err)
	require.Equal(t, "c2", lease.HolderID)
}

func TestStoreReleaseCoordinatorLease(t *testing.T) {
	store, err := Open(t.TempDir(), nil)
	require.NoError(t, err)

	_, err = campaignLease(store, "c1", 1_000, 100, 10, 20, 30, "")
	require.NoError(t, err)

	lease, err := releaseLease(store, "c1", 200, 30, 40)
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, int64(200), lease.ExpiresUnixNano)
	require.False(t, lease.ActiveAt(200))

	_, err = releaseLease(store, "c2", 250, 30, 40)
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseOwner))
}

func TestStoreSealCoordinatorLease(t *testing.T) {
	store, err := Open(t.TempDir(), nil)
	require.NoError(t, err)

	_, err = campaignLease(store, "c1", 1_000, 100, 10, 20, 30, "")
	require.NoError(t, err)

	seal, err := sealLease(store, "c1", 200, controlplane.Frontiers(12, 34, 56))
	require.NoError(t, err)
	require.Equal(t, "c1", seal.HolderID)
	require.Equal(t, uint64(1), seal.CertGeneration)
	require.Equal(t, uint32(rootstate.CoordinatorDutyMaskDefault), seal.DutyMask)
	require.Equal(t, uint64(12), seal.Frontiers.Frontier(rootstate.CoordinatorDutyAllocID))
	require.Equal(t, uint64(34), seal.Frontiers.Frontier(rootstate.CoordinatorDutyTSO))
	require.Equal(t, uint64(56), seal.Frontiers.Frontier(rootstate.CoordinatorDutyGetRegionByKey))
	require.NotEqual(t, rootstate.Cursor{}, seal.SealedAtCursor)

	state, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, seal, state.CoordinatorSeal)

	sealedAgain, err := sealLease(store, "c1", 250, controlplane.Frontiers(99, 99, 99))
	require.NoError(t, err)
	require.Equal(t, seal, sealedAgain)

	renewed, err := campaignLease(store, "c1", 1_200, 250, 20, 40, 56, rootstate.CoordinatorSealDigest(seal))
	require.NoError(t, err)
	require.Equal(t, uint64(2), renewed.CertGeneration)
	require.Equal(t, rootstate.CoordinatorSealDigest(seal), renewed.PredecessorDigest)

	_, err = campaignLease(store, "c1", 1_200, 250, 20, 40, 56, "")
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseLineage))

	other, err := campaignLease(store, "c2", 1_300, 300, 30, 50, 56, "")
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseHeld))
	require.Equal(t, renewed, other)
}

func TestStoreCampaignCoordinatorLeaseRequiresCoverageAfterSeal(t *testing.T) {
	store, err := Open(t.TempDir(), nil)
	require.NoError(t, err)

	_, err = campaignLease(store, "c1", 1_000, 100, 10, 20, 30, "")
	require.NoError(t, err)
	seal, err := sealLease(store, "c1", 200, controlplane.Frontiers(12, 34, 56))
	require.NoError(t, err)

	held, err := campaignLease(store, "c1", 1_200, 250, 11, 34, 56, rootstate.CoordinatorSealDigest(seal))
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage))
	require.Equal(t, uint64(1), held.CertGeneration)

	held, err = campaignLease(store, "c1", 1_200, 250, 12, 33, 56, rootstate.CoordinatorSealDigest(seal))
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage))
	require.Equal(t, uint64(1), held.CertGeneration)

	held, err = campaignLease(store, "c1", 1_200, 250, 12, 34, 55, rootstate.CoordinatorSealDigest(seal))
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage))
	require.Equal(t, uint64(1), held.CertGeneration)
}

func TestStoreConfirmCoordinatorClosure(t *testing.T) {
	store, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	_, err = store.Append(rootevent.RegionDescriptorPublished(testDescriptor(1, []byte("a"), []byte("z"))))
	require.NoError(t, err)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err = store.Append(rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	_, err = campaignLease(store, "c1", 1_000, 100, 10, 20, 30, "")
	require.NoError(t, err)
	seal, err := sealLease(store, "c1", 200, controlplane.Frontiers(12, 34, 56))
	require.NoError(t, err)

	_, err = confirmClosure(store, "c1", 250)
	require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseAudit)

	lease, err := campaignLease(store, "c1", 1_200, 250, 12, 34, 56, rootstate.CoordinatorSealDigest(seal))
	require.NoError(t, err)

	closure, err := confirmClosure(store, "c1", 260)
	require.NoError(t, err)
	require.Equal(t, "c1", closure.HolderID)
	require.Equal(t, seal.CertGeneration, closure.SealGeneration)
	require.Equal(t, lease.CertGeneration, closure.SuccessorGeneration)
	require.Equal(t, rootstate.CoordinatorSealDigest(seal), closure.SealDigest)
	require.Equal(t, rootstate.CoordinatorClosureStageConfirmed, closure.Stage)

	state, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, closure, state.CoordinatorClosure)
}

func TestStoreReattachCoordinatorClosure(t *testing.T) {
	store, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err = store.Append(rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	_, err = campaignLease(store, "c1", 1_000, 100, 10, 20, 56, "")
	require.NoError(t, err)
	seal, err := sealLease(store, "c1", 200, controlplane.Frontiers(12, 34, 56))
	require.NoError(t, err)
	_, err = campaignLease(store, "c1", 1_200, 250, 12, 34, 56, rootstate.CoordinatorSealDigest(seal))
	require.NoError(t, err)

	_, err = reattachClosure(store, "c1", 255)
	require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseReattach)

	confirmed, err := confirmClosure(store, "c1", 260)
	require.NoError(t, err)
	closed, err := closeClosure(store, "c1", 265)
	require.NoError(t, err)
	reattached, err := reattachClosure(store, "c1", 270)
	require.NoError(t, err)
	require.Equal(t, "c1", reattached.HolderID)
	require.Equal(t, closed.SuccessorGeneration, reattached.SuccessorGeneration)
	require.Equal(t, closed.SealGeneration, reattached.SealGeneration)
	require.Equal(t, closed.SealDigest, reattached.SealDigest)
	require.Equal(t, rootstate.CoordinatorClosureStageReattached, reattached.Stage)

	state, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, reattached, state.CoordinatorClosure)
	require.Equal(t, rootstate.CoordinatorClosureStageConfirmed, confirmed.Stage)
}

func TestStoreIgnoresTruncatedLogTail(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)
	_, err = store.Append(rootevent.StoreJoined(1, "s1"))
	require.NoError(t, err)

	f, err := os.OpenFile(filepath.Join(dir, rootfile.LogFileName), os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	events, tail, err := reopened.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 0)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, tail)
}

func TestStoreReplaysLogAfterStaleCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)
	commit, err := store.Append(rootevent.PeerAdded(1, 2, 3, testDescriptor(1, []byte("a"), []byte("z"))))
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, commit.Cursor)

	payload, err := proto.Marshal(&metapb.RootState{})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, rootfile.CheckpointFileName), payload, 0o644))

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, state.LastCommitted)
}

func TestStoreRejectsLegacyRootStateCheckpoint(t *testing.T) {
	dir := t.TempDir()
	payload, err := proto.Marshal(&metapb.RootState{
		ClusterEpoch:    7,
		MembershipEpoch: 3,
		LastCommitted:   &metapb.RootCursor{Term: 1, Index: 4},
		IdFence:         11,
		TsoFence:        22,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, rootfile.CheckpointFileName), payload, 0o644))

	_, err = Open(dir, nil)
	require.Error(t, err)
}

func TestStoreCompactsPhysicalLogAndKeepsRecentTail(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)

	total := maxRetainedRecords + 8
	for i := range total {
		_, err := store.Append(rootevent.RegionDescriptorPublished(testDescriptor(uint64(100+i), []byte{byte('a' + i%26)}, []byte{byte('b' + i%26)})))
		require.NoError(t, err)
	}

	reopened, err := Open(dir, nil)
	require.NoError(t, err)

	tailCursor := rootstate.Cursor{Term: 1, Index: uint64(total - maxRetainedRecords)}
	events, tail, err := reopened.ReadSince(tailCursor)
	require.NoError(t, err)
	require.Len(t, events, maxRetainedRecords)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: uint64(total)}, tail)

	events, tail, err = reopened.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, total)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: uint64(total)}, tail)
}

func testDescriptor(regionID uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  regionID,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: regionID*10 + 1}, {StoreID: 2, PeerID: regionID*10 + 2}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
