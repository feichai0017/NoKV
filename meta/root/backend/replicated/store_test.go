package replicated

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func campaignLease(store *Store, holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64, predecessorDigest string) (rootstate.CoordinatorLease, error) {
	state, err := store.ApplyCoordinatorLease(context.Background(), rootproto.CoordinatorLeaseCommand{
		Kind:              rootproto.CoordinatorLeaseCommandIssue,
		HolderID:          holderID,
		ExpiresUnixNano:   expiresUnixNano,
		NowUnixNano:       nowUnixNano,
		PredecessorDigest: predecessorDigest,
		HandoffFrontiers:  controlplane.Frontiers(rootstate.State{IDFence: idFence, TSOFence: tsoFence}, descriptorRevision),
	})
	return state.Lease, err
}

func releaseLease(store *Store, holderID string, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error) {
	state, err := store.ApplyCoordinatorLease(context.Background(), rootproto.CoordinatorLeaseCommand{
		Kind:             rootproto.CoordinatorLeaseCommandRelease,
		HolderID:         holderID,
		NowUnixNano:      nowUnixNano,
		HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: idFence, TSOFence: tsoFence}, 0),
	})
	return state.Lease, err
}

func sealLease(store *Store, holderID string, nowUnixNano int64, frontiers rootproto.CoordinatorDutyFrontiers) (rootstate.CoordinatorSeal, error) {
	state, err := store.ApplyCoordinatorClosure(context.Background(), rootproto.CoordinatorClosureCommand{
		Kind:        rootproto.CoordinatorClosureCommandSeal,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	return state.Seal, err
}

func confirmClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(context.Background(), rootproto.CoordinatorClosureCommand{
		Kind:        rootproto.CoordinatorClosureCommandConfirm,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func closeClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(context.Background(), rootproto.CoordinatorClosureCommand{
		Kind:        rootproto.CoordinatorClosureCommandClose,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func reattachClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.CoordinatorClosure, error) {
	state, err := store.ApplyCoordinatorClosure(context.Background(), rootproto.CoordinatorClosureCommand{
		Kind:        rootproto.CoordinatorClosureCommandReattach,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Closure, err
}

func TestReplicatedStoreAppendAndReopen(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 4)

	commit, err := stores[leaderID].Append(context.Background(),
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), commit.State.MembershipEpoch)
	require.Equal(t, uint64(1), commit.State.ClusterEpoch)

	reopened, err := Open(Config{Driver: drivers[leaderID], MaxRetainedRecords: 4})
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, commit.State, state)
	events, tail, err := reopened.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, uint64(10), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, commit.Cursor, tail)
}

func TestReplicatedStoreRequiresLogAndCheckpoint(t *testing.T) {
	_, err := Open(Config{})
	require.Error(t, err)
}

func TestReplicatedStoreInstallBootstrapReplacesState(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 4)

	_, err := stores[leaderID].Append(context.Background(),
		rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("b"))),
	)
	require.NoError(t, err)

	snapshot := rootstate.Snapshot{
		State: rootstate.State{
			ClusterEpoch:  7,
			LastCommitted: rootstate.Cursor{Term: 3, Index: 9},
			IDFence:       123,
			TSOFence:      456,
		},
		Descriptors: map[uint64]descriptor.Descriptor{
			99: testDescriptor(99, []byte("m"), []byte("z")),
		},
	}
	require.NoError(t, stores[leaderID].InstallBootstrap(rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{
			Snapshot: snapshot,
		},
	}))

	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, snapshot.State, current)

	events, tail, err := stores[leaderID].ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, uint64(99), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, snapshot.State.LastCommitted, tail)

	reopened, err := Open(Config{Driver: drivers[leaderID], MaxRetainedRecords: 4})
	require.NoError(t, err)
	current, err = reopened.Current()
	require.NoError(t, err)
	require.Equal(t, snapshot.State, current)

	observed, err := rootstorage.ObserveCommitted(drivers[leaderID], 0)
	require.NoError(t, err)
	require.Equal(t, snapshot.State, observed.Checkpoint.Snapshot.State)
	require.Empty(t, observed.Tail.Records)
}

func TestOpenAcceptsDriverConfig(t *testing.T) {
	_, drivers, _ := openNetworkTestCluster(t, 3)
	store, err := Open(Config{Driver: drivers[1], MaxRetainedRecords: 3})
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestReplicatedStoreWaitForTailTracksFollowerAdvance(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	waitDone := make(chan rootstorage.TailAdvance, 1)
	waitErr := make(chan error, 1)
	go func() {
		advance, err := stores[followerID].WaitForTail(rootstorage.TailToken{}, 5*time.Second)
		if err != nil {
			waitErr <- err
			return
		}
		waitDone <- advance
	}()

	commit, err := stores[leaderID].Append(context.Background(),
		rootevent.RegionDescriptorPublished(testDescriptor(77, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)

	select {
	case err := <-waitErr:
		require.NoError(t, err)
	case advance := <-waitDone:
		require.True(t, advance.Advanced())
		require.Equal(t, rootstorage.TailAdvanceCursorAdvanced, advance.Kind())
		require.Equal(t, commit.Cursor, advance.Token.Cursor)
		require.NotEmpty(t, advance.Observed.Tail.Records)
		require.Equal(t, commit.Cursor, advance.LastCursor())
	case <-time.After(6 * time.Second):
		t.Fatal("timed out waiting for replicated tail advance")
	}
}

func TestNetworkDriverWaitForTailReturnsObservedStateOnClose(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 4)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	commit, err := stores[leaderID].Append(context.Background(),
		rootevent.RegionDescriptorPublished(testDescriptor(91, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		return err == nil && reflect.DeepEqual(current, commit.State)
	}, 5*time.Second, 50*time.Millisecond)

	currentAdvance, err := drivers[followerID].WaitForTail(rootstorage.TailToken{}, 50*time.Millisecond)
	require.NoError(t, err)

	require.NoError(t, drivers[followerID].Close())

	advance, err := drivers[followerID].WaitForTail(currentAdvance.Token, 50*time.Millisecond)
	if err != nil {
		require.ErrorContains(t, err, "closed")
	}
	require.Equal(t, commit.Cursor, advance.LastCursor())
	require.NotEmpty(t, advance.Observed.Tail.Records)
}

func TestReplicatedStoreLeaderAndTailWrappers(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	require.True(t, stores[leaderID].IsLeader())
	require.False(t, stores[followerID].IsLeader())
	require.Equal(t, leaderID, stores[followerID].LeaderID())
	require.NotNil(t, stores[followerID].TailNotify())

	subscription := stores[followerID].SubscribeTail(rootstorage.TailToken{})
	require.NotNil(t, subscription)

	commit, err := stores[leaderID].Append(context.Background(),
		rootevent.RegionDescriptorPublished(testDescriptor(101, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)

	advance, err := subscription.Next(context.Background(), 2*time.Second)
	require.NoError(t, err)
	require.True(t, advance.Advanced())
	require.Equal(t, commit.Cursor, advance.LastCursor())

	subscription.Acknowledge(advance)
	next, err := stores[followerID].ObserveTail(subscription.Token())
	require.NoError(t, err)
	require.False(t, next.Advanced())
}

func TestReplicatedStoreFenceAllocator(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	idFence, err := stores[leaderID].FenceAllocator(context.Background(), rootstate.AllocatorKindID, 123)
	require.NoError(t, err)
	require.Equal(t, uint64(123), idFence)

	tsoFence, err := stores[leaderID].FenceAllocator(context.Background(), rootstate.AllocatorKindTSO, 456)
	require.NoError(t, err)
	require.Equal(t, uint64(456), tsoFence)

	idFence, err = stores[leaderID].FenceAllocator(context.Background(), rootstate.AllocatorKindID, 120)
	require.NoError(t, err)
	require.Equal(t, uint64(123), idFence)

	_, err = stores[leaderID].FenceAllocator(context.Background(), rootstate.AllocatorKind(99), 1)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		if err != nil {
			return false
		}
		return current.IDFence == 123 && current.TSOFence == 456
	}, 5*time.Second, 50*time.Millisecond)
}

func TestReplicatedStoreCampaignCoordinatorLease(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	lease, err := campaignLease(stores[leaderID], "c1", 1_000, 100, 123, 456, 1, "")
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.CertGeneration)
	require.Equal(t, uint32(rootproto.CoordinatorDutyMaskDefault), lease.DutyMask)
	require.NotEqual(t, rootstate.Cursor{}, lease.IssuedCursor)

	_, err = campaignLease(stores[leaderID], "c2", 1_500, 200, 200, 500, 1, "")
	require.Error(t, err)

	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		if err != nil {
			return false
		}
		return current.CoordinatorLease.HolderID == "c1" &&
			current.CoordinatorLease.CertGeneration == 1 &&
			current.IDFence == 123 &&
			current.TSOFence == 456
	}, 5*time.Second, 50*time.Millisecond)
}

func TestReplicatedStoreConfirmCoordinatorClosure(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err := stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	_, err = campaignLease(stores[leaderID], "c1", 1_000, 100, 10, 20, 56, "")
	require.NoError(t, err)
	seal, err := sealLease(stores[leaderID], "c1", 200, controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56))
	require.NoError(t, err)
	lease, err := campaignLease(stores[leaderID], "c1", 1_200, 250, 12, 34, 56, rootstate.CoordinatorSealDigest(seal))
	require.NoError(t, err)

	closure, err := confirmClosure(stores[leaderID], "c1", 260)
	require.NoError(t, err)
	require.Equal(t, seal.CertGeneration, closure.SealGeneration)
	require.Equal(t, lease.CertGeneration, closure.SuccessorGeneration)
	require.Equal(t, rootstate.CoordinatorSealDigest(seal), closure.SealDigest)
	require.Equal(t, rootproto.CoordinatorClosureStageConfirmed, closure.Stage)
	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, closure, current.CoordinatorClosure)
}

func TestReplicatedStoreReattachCoordinatorClosure(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err := stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	_, err = campaignLease(stores[leaderID], "c1", 1_000, 100, 10, 20, 56, "")
	require.NoError(t, err)
	seal, err := sealLease(stores[leaderID], "c1", 200, controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56))
	require.NoError(t, err)
	_, err = campaignLease(stores[leaderID], "c1", 1_200, 250, 12, 34, 56, rootstate.CoordinatorSealDigest(seal))
	require.NoError(t, err)

	_, err = reattachClosure(stores[leaderID], "c1", 255)
	require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseReattach)

	confirmed, err := confirmClosure(stores[leaderID], "c1", 260)
	require.NoError(t, err)
	closed, err := closeClosure(stores[leaderID], "c1", 265)
	require.NoError(t, err)
	reattached, err := reattachClosure(stores[leaderID], "c1", 270)
	require.NoError(t, err)
	require.Equal(t, closed.SuccessorGeneration, reattached.SuccessorGeneration)
	require.Equal(t, closed.SealGeneration, reattached.SealGeneration)
	require.Equal(t, closed.SealDigest, reattached.SealDigest)
	require.Equal(t, rootproto.CoordinatorClosureStageReattached, reattached.Stage)

	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, reattached, current.CoordinatorClosure)
	require.Equal(t, rootproto.CoordinatorClosureStageConfirmed, confirmed.Stage)
}

func TestReplicatedStoreCoordinatorLeaseFenceSurvivesLeaderChange(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 8)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	lease, err := campaignLease(stores[leaderID], "c1", 1_000, 100, 123, 456, 1, "")
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.CertGeneration)
	require.NotEqual(t, rootstate.Cursor{}, lease.IssuedCursor)

	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		return err == nil &&
			current.CoordinatorLease.HolderID == "c1" &&
			current.CoordinatorLease.CertGeneration == 1 &&
			current.IDFence == 123 &&
			current.TSOFence == 456
	}, 5*time.Second, 50*time.Millisecond)

	initialIssued := lease.IssuedCursor

	require.NoError(t, drivers[followerID].Campaign())
	require.Eventually(t, func() bool {
		return drivers[followerID].IsLeader()
	}, 5*time.Second, 50*time.Millisecond)

	renewed, err := campaignLease(stores[followerID], "c1", 2_000, 500, 200, 600, 1, "")
	require.NoError(t, err)
	require.Equal(t, "c1", renewed.HolderID)
	require.Equal(t, uint64(1), renewed.CertGeneration)
	require.Equal(t, initialIssued, renewed.IssuedCursor)

	for _, id := range []uint64{1, 2, 3} {
		require.Eventually(t, func() bool {
			if err := stores[id].Refresh(); err != nil {
				return false
			}
			current, err := stores[id].Current()
			return err == nil &&
				current.CoordinatorLease.HolderID == "c1" &&
				current.CoordinatorLease.CertGeneration == 1 &&
				current.CoordinatorLease.IssuedCursor == initialIssued &&
				current.IDFence == 200 &&
				current.TSOFence == 600
		}, 5*time.Second, 50*time.Millisecond)
	}
}

func TestReplicatedStoreReleaseCoordinatorLease(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)

	_, err := campaignLease(stores[leaderID], "c1", 1_000, 100, 123, 456, 1, "")
	require.NoError(t, err)

	lease, err := releaseLease(stores[leaderID], "c1", 200, 300, 500)
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.CertGeneration)
	require.Equal(t, int64(200), lease.ExpiresUnixNano)

	_, err = releaseLease(stores[leaderID], "c2", 250, 300, 500)
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseOwner))
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
