package replicated

import (
	"context"
	"reflect"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/meta/topology"
	"github.com/stretchr/testify/require"
)

func issueGrant(store *Store, holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence, descriptorRevision uint64) (rootproto.AuthorityGrant, rootproto.GrantCertificate, error) {
	state, cert, err := store.ApplyGrant(context.Background(), rootproto.GrantCommand{
		Kind:            rootproto.GrantActIssue,
		HolderID:        holderID,
		ExpiresUnixNano: expiresUnixNano,
		NowUnixNano:     nowUnixNano,
		RequestedDuties: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, idFence),
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, tsoFence),
			rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, descriptorRevision, 0),
		},
	})
	return state.ActiveGrant, cert, err
}

func issueGrantWithDuties(store *Store, holderID string, expiresUnixNano, nowUnixNano int64, duties []rootproto.DutyGrant) (rootproto.AuthorityGrant, error) {
	state, _, err := store.ApplyGrant(context.Background(), rootproto.GrantCommand{
		Kind:            rootproto.GrantActIssue,
		HolderID:        holderID,
		ExpiresUnixNano: expiresUnixNano,
		NowUnixNano:     nowUnixNano,
		RequestedDuties: duties,
	})
	return state.ActiveGrant, err
}

func sealGrant(store *Store, holderID, grantID string, usages []rootproto.AuthorityUsage) (rootproto.GrantRetirement, error) {
	state, _, err := store.ApplyGrant(context.Background(), rootproto.GrantCommand{
		Kind:        rootproto.GrantActSeal,
		HolderID:    holderID,
		GrantID:     grantID,
		ExactUsages: usages,
	})
	if len(state.RetiredGrants) == 0 {
		return rootproto.GrantRetirement{}, err
	}
	return state.RetiredGrants[len(state.RetiredGrants)-1], err
}

func retireExpiredGrant(store *Store, grantID string, nowUnixNano int64) (rootproto.GrantRetirement, error) {
	state, _, err := store.ApplyGrant(context.Background(), rootproto.GrantCommand{
		Kind:        rootproto.GrantActRetireExpired,
		GrantID:     grantID,
		NowUnixNano: nowUnixNano,
	})
	if len(state.RetiredGrants) == 0 {
		return rootproto.GrantRetirement{}, err
	}
	return state.RetiredGrants[len(state.RetiredGrants)-1], err
}

func inheritGrant(store *Store, holderID string, predecessors ...string) (rootstate.EunomiaState, error) {
	state, _, err := store.ApplyGrant(context.Background(), rootproto.GrantCommand{
		Kind:                rootproto.GrantActInherit,
		HolderID:            holderID,
		PredecessorGrantIDs: predecessors,
	})
	return state, err
}

func TestReplicatedStoreAppendAndReopen(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 4)

	commit, err := stores[leaderID].Append(context.Background(),
		rootevent.StoreJoined(1),
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
		Descriptors: map[uint64]topology.Descriptor{
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

func TestReplicatedStoreIssueGrant(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	grant, cert, err := issueGrant(stores[leaderID], "c1", 1_000, 100, 123, 456, 1)
	require.NoError(t, err)
	require.Equal(t, "c1", grant.HolderID)
	require.Equal(t, uint64(1), grant.Era)
	require.Equal(t, "c1/1", grant.GrantID)
	require.NotEqual(t, rootstate.Cursor{}, grant.IssuedAt)
	require.Equal(t, grant, cert.Grant)
	require.Equal(t, rootproto.GrantSignerKeyID, cert.SignerKeyID)
	require.NotEmpty(t, cert.Signature)

	_, _, err = issueGrant(stores[leaderID], "c2", 1_500, 200, 200, 500, 1)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		if err != nil {
			return false
		}
		return current.ActiveGrant.HolderID == "c1" &&
			current.ActiveGrant.Era == 1 &&
			current.IDFence == 123 &&
			current.TSOFence == 456
	}, 5*time.Second, 50*time.Millisecond)
}

func TestReplicatedStoreSealAndInheritGrant(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err := stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)

	grant, _, err := issueGrant(stores[leaderID], "c1", 1_000, 100, 10, 20, 56)
	require.NoError(t, err)
	seal, err := sealGrant(stores[leaderID], "c1", grant.GrantID, []rootproto.AuthorityUsage{
		{DutyID: rootproto.DutyAllocID, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 10}},
		{DutyID: rootproto.DutyTSO, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 20}},
		{DutyID: rootproto.DutyRegionLookup, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: 56}},
	})
	require.NoError(t, err)
	require.Equal(t, grant.GrantID, seal.GrantID)
	require.Equal(t, rootproto.GrantRetirementSealedExact, seal.Mode)

	successor, err := issueGrantWithDuties(stores[leaderID], "c1", 1_200, 250, []rootproto.DutyGrant{
		rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 12),
		rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 34),
		rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 56, 0),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), successor.Era)
	require.Len(t, successor.PredecessorRetirements, 1)
	require.Equal(t, seal.GrantID, successor.PredecessorRetirements[0].GrantID)

	state, err := inheritGrant(stores[leaderID], "c1", seal.GrantID)
	require.NoError(t, err)
	require.Len(t, state.GrantInheritances, 1)
	require.Equal(t, seal.GrantID, state.GrantInheritances[0].PredecessorGrantID)
	require.Equal(t, successor.GrantID, state.GrantInheritances[0].SuccessorGrantID)
	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, successor.GrantID, current.RetiredGrants[0].InheritedByGrantID)
}

func TestReplicatedStoreRejectsSuccessorWithoutRetirementCoverage(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)

	grant, _, err := issueGrant(stores[leaderID], "c1", 1_000, 100, 100, 200, 1)
	require.NoError(t, err)
	_, err = sealGrant(stores[leaderID], "c1", grant.GrantID, nil)
	require.NoError(t, err)

	_, err = issueGrantWithDuties(stores[leaderID], "c2", 1_500, 300, []rootproto.DutyGrant{
		rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 99),
		rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200),
		rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 1, 0),
	})
	require.ErrorIs(t, err, rootstate.ErrInheritance)

	successor, err := issueGrantWithDuties(stores[leaderID], "c2", 1_500, 300, []rootproto.DutyGrant{
		rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100),
		rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200),
		rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 1, 0),
	})
	require.NoError(t, err)
	require.Equal(t, "c2", successor.HolderID)
	require.Len(t, successor.PredecessorRetirements, 1)
}

func TestReplicatedStoreGrantFenceSurvivesLeaderChange(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 8)
	followerID := uint64(1)
	if followerID == leaderID {
		followerID = 2
	}

	grant, _, err := issueGrant(stores[leaderID], "c1", 1_000, 100, 123, 456, 1)
	require.NoError(t, err)
	require.Equal(t, "c1", grant.HolderID)
	require.Equal(t, uint64(1), grant.Era)
	require.NotEqual(t, rootstate.Cursor{}, grant.IssuedAt)

	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		return err == nil &&
			current.ActiveGrant.HolderID == "c1" &&
			current.ActiveGrant.Era == 1 &&
			current.IDFence == 123 &&
			current.TSOFence == 456
	}, 5*time.Second, 50*time.Millisecond)

	drivers[leaderID].PauseTicks()
	defer drivers[leaderID].ResumeTicks()
	require.NoError(t, drivers[followerID].Campaign())
	require.Eventually(t, func() bool {
		return drivers[followerID].IsLeader()
	}, 5*time.Second, 50*time.Millisecond)

	renewed, _, err := issueGrant(stores[followerID], "c1", 2_000, 500, 200, 600, 1)
	require.NoError(t, err)
	require.Equal(t, "c1", renewed.HolderID)
	require.Equal(t, uint64(2), renewed.Era)
	require.Len(t, renewed.PredecessorRetirements, 1)

	for _, id := range []uint64{1, 2, 3} {
		require.Eventually(t, func() bool {
			if err := stores[id].Refresh(); err != nil {
				return false
			}
			current, err := stores[id].Current()
			return err == nil &&
				current.ActiveGrant.HolderID == "c1" &&
				current.ActiveGrant.Era == 2 &&
				current.IDFence == 200 &&
				current.TSOFence == 600
		}, 5*time.Second, 50*time.Millisecond)
	}
}

func TestReplicatedStoreRetireExpiredGrant(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)

	grant, _, err := issueGrant(stores[leaderID], "c1", 1_000, 100, 123, 456, 1)
	require.NoError(t, err)

	_, err = retireExpiredGrant(stores[leaderID], grant.GrantID, 999)
	require.ErrorIs(t, err, rootstate.ErrPrimacy)

	retired, err := retireExpiredGrant(stores[leaderID], grant.GrantID, 1_001)
	require.NoError(t, err)
	require.Equal(t, grant.GrantID, retired.GrantID)
	require.Equal(t, rootproto.GrantRetirementExpiredBound, retired.Mode)
	require.Equal(t, grant.Duties, retired.Bounds)

	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.False(t, current.ActiveGrant.Present())
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
