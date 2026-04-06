package replicated

import (
	"context"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestReplicatedStoreAppendAndReopen(t *testing.T) {
	stores, drivers, leaderID := openNetworkTestCluster(t, 4)

	commit, err := stores[leaderID].Append(
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

	_, err := stores[leaderID].Append(
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

	commit, err := stores[leaderID].Append(
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

	commit, err := stores[leaderID].Append(
		rootevent.RegionDescriptorPublished(testDescriptor(91, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		if err := stores[followerID].Refresh(); err != nil {
			return false
		}
		current, err := stores[followerID].Current()
		return err == nil && current == commit.State
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

	commit, err := stores[leaderID].Append(
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

	idFence, err := stores[leaderID].FenceAllocator(rootpkg.AllocatorKindID, 123)
	require.NoError(t, err)
	require.Equal(t, uint64(123), idFence)

	tsoFence, err := stores[leaderID].FenceAllocator(rootpkg.AllocatorKindTSO, 456)
	require.NoError(t, err)
	require.Equal(t, uint64(456), tsoFence)

	idFence, err = stores[leaderID].FenceAllocator(rootpkg.AllocatorKindID, 120)
	require.NoError(t, err)
	require.Equal(t, uint64(123), idFence)

	_, err = stores[leaderID].FenceAllocator(rootpkg.AllocatorKind(99), 1)
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
