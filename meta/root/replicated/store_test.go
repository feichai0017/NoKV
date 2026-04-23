package replicated

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func campaignLease(store *Store, holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64, lineageDigest string) (rootstate.Tenure, error) {
	state, err := store.ApplyTenure(context.Background(), rootproto.TenureCommand{
		Kind:               rootproto.TenureActIssue,
		HolderID:           holderID,
		ExpiresUnixNano:    expiresUnixNano,
		NowUnixNano:        nowUnixNano,
		LineageDigest:      lineageDigest,
		InheritedFrontiers: succession.Frontiers(rootstate.State{IDFence: idFence, TSOFence: tsoFence}, descriptorRevision),
	})
	return state.Tenure, err
}

func releaseLease(store *Store, holderID string, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.Tenure, error) {
	state, err := store.ApplyTenure(context.Background(), rootproto.TenureCommand{
		Kind:               rootproto.TenureActRelease,
		HolderID:           holderID,
		NowUnixNano:        nowUnixNano,
		InheritedFrontiers: succession.Frontiers(rootstate.State{IDFence: idFence, TSOFence: tsoFence}, 0),
	})
	return state.Tenure, err
}

func sealLease(store *Store, holderID string, nowUnixNano int64, frontiers rootproto.MandateFrontiers) (rootstate.Legacy, error) {
	state, err := store.ApplyTransit(context.Background(), rootproto.TransitCommand{
		Kind:        rootproto.TransitActSeal,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	return state.Legacy, err
}

func confirmClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.Transit, error) {
	state, err := store.ApplyTransit(context.Background(), rootproto.TransitCommand{
		Kind:        rootproto.TransitActConfirm,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Transit, err
}

func closeClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.Transit, error) {
	state, err := store.ApplyTransit(context.Background(), rootproto.TransitCommand{
		Kind:        rootproto.TransitActClose,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Transit, err
}

func reattachClosure(store *Store, holderID string, nowUnixNano int64) (rootstate.Transit, error) {
	state, err := store.ApplyTransit(context.Background(), rootproto.TransitCommand{
		Kind:        rootproto.TransitActReattach,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
	})
	return state.Transit, err
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

func TestReplicatedStoreCampaignTenure(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	lease, err := campaignLease(stores[leaderID], "c1", 1_000, 100, 123, 456, 1, "")
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.Epoch)
	require.Equal(t, uint32(rootproto.MandateDefault), lease.Mandate)
	require.NotEqual(t, rootstate.Cursor{}, lease.IssuedAt)

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
		return current.Tenure.HolderID == "c1" &&
			current.Tenure.Epoch == 1 &&
			current.IDFence == 123 &&
			current.TSOFence == 456
	}, 5*time.Second, 50*time.Millisecond)
}

func TestReplicatedStoreConfirmTransit(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err := stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	_, err = campaignLease(stores[leaderID], "c1", 1_000, 100, 10, 20, 56, "")
	require.NoError(t, err)
	seal, err := sealLease(stores[leaderID], "c1", 200, succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56))
	require.NoError(t, err)
	lease, err := campaignLease(stores[leaderID], "c1", 1_200, 250, 12, 34, 56, rootstate.DigestOfLegacy(seal))
	require.NoError(t, err)

	closure, err := confirmClosure(stores[leaderID], "c1", 260)
	require.NoError(t, err)
	require.Equal(t, seal.Epoch, closure.LegacyEpoch)
	require.Equal(t, lease.Epoch, closure.SuccessorEpoch)
	require.Equal(t, rootstate.DigestOfLegacy(seal), closure.LegacyDigest)
	require.Equal(t, rootproto.TransitStageConfirmed, closure.Stage)
	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, closure, current.Transit)
}

func TestReplicatedStoreReattachTransit(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err := stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	_, err = campaignLease(stores[leaderID], "c1", 1_000, 100, 10, 20, 56, "")
	require.NoError(t, err)
	seal, err := sealLease(stores[leaderID], "c1", 200, succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56))
	require.NoError(t, err)
	_, err = campaignLease(stores[leaderID], "c1", 1_200, 250, 12, 34, 56, rootstate.DigestOfLegacy(seal))
	require.NoError(t, err)

	_, err = reattachClosure(stores[leaderID], "c1", 255)
	require.ErrorIs(t, err, rootstate.ErrClosure)

	confirmed, err := confirmClosure(stores[leaderID], "c1", 260)
	require.NoError(t, err)
	closed, err := closeClosure(stores[leaderID], "c1", 265)
	require.NoError(t, err)
	reattached, err := reattachClosure(stores[leaderID], "c1", 270)
	require.NoError(t, err)
	require.Equal(t, closed.SuccessorEpoch, reattached.SuccessorEpoch)
	require.Equal(t, closed.LegacyEpoch, reattached.LegacyEpoch)
	require.Equal(t, closed.LegacyDigest, reattached.LegacyDigest)
	require.Equal(t, rootproto.TransitStageReattached, reattached.Stage)

	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, reattached, current.Transit)
	require.Equal(t, rootproto.TransitStageConfirmed, confirmed.Stage)
}

func TestReplicatedStoreTenureFenceSurvivesLeaderChange(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 8)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	lease, err := campaignLease(stores[leaderID], "c1", 1_000, 100, 123, 456, 1, "")
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.Epoch)
	require.NotEqual(t, rootstate.Cursor{}, lease.IssuedAt)

	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		return err == nil &&
			current.Tenure.HolderID == "c1" &&
			current.Tenure.Epoch == 1 &&
			current.IDFence == 123 &&
			current.TSOFence == 456
	}, 5*time.Second, 50*time.Millisecond)

	initialIssued := lease.IssuedAt

	require.NoError(t, drivers[followerID].Campaign())
	require.Eventually(t, func() bool {
		return drivers[followerID].IsLeader()
	}, 5*time.Second, 50*time.Millisecond)

	renewed, err := campaignLease(stores[followerID], "c1", 2_000, 500, 200, 600, 1, "")
	require.NoError(t, err)
	require.Equal(t, "c1", renewed.HolderID)
	require.Equal(t, uint64(1), renewed.Epoch)
	require.Equal(t, initialIssued, renewed.IssuedAt)

	for _, id := range []uint64{1, 2, 3} {
		require.Eventually(t, func() bool {
			if err := stores[id].Refresh(); err != nil {
				return false
			}
			current, err := stores[id].Current()
			return err == nil &&
				current.Tenure.HolderID == "c1" &&
				current.Tenure.Epoch == 1 &&
				current.Tenure.IssuedAt == initialIssued &&
				current.IDFence == 200 &&
				current.TSOFence == 600
		}, 5*time.Second, 50*time.Millisecond)
	}
}

func TestReplicatedStoreReleaseTenure(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)

	_, err := campaignLease(stores[leaderID], "c1", 1_000, 100, 123, 456, 1, "")
	require.NoError(t, err)

	lease, err := releaseLease(stores[leaderID], "c1", 200, 300, 500)
	require.NoError(t, err)
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.Epoch)
	require.Equal(t, int64(200), lease.ExpiresUnixNano)

	_, err = releaseLease(stores[leaderID], "c2", 250, 300, 500)
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrPrimacy))
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
