package replicated

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReplicatedStoreAppendAndReopen(t *testing.T) {
	store, driver, err := OpenMemory(4)
	require.NoError(t, err)

	commit, err := store.Append(
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, commit.Cursor)
	require.Equal(t, uint64(1), commit.State.MembershipEpoch)
	require.Equal(t, uint64(1), commit.State.ClusterEpoch)

	reopened, err := Open(driver.Config(4))
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, commit.State, state)
	events, tail, err := reopened.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, rootevent.KindRegionDescriptorPublished, events[0].Kind)
	require.Equal(t, uint64(10), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, commit.Cursor, tail)
}

func TestReplicatedStoreRequiresLogAndCheckpoint(t *testing.T) {
	_, err := Open(Config{})
	require.Error(t, err)
	driver := NewMemoryDriver()
	_, err = Open(Config{Log: driver.Log()})
	require.Error(t, err)
	_, err = Open(Config{Checkpoint: driver.CheckpointStore()})
	require.Error(t, err)
}

func TestReplicatedStoreCompactsMemoryDriverTail(t *testing.T) {
	store, driver, err := OpenMemory(2)
	require.NoError(t, err)

	_, err = store.Append(
		rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("b"))),
		rootevent.RegionDescriptorPublished(testDescriptor(11, []byte("b"), []byte("c"))),
		rootevent.RegionDescriptorPublished(testDescriptor(12, []byte("c"), []byte("d"))),
	)
	require.NoError(t, err)

	reopened, err := Open(driver.Config(2))
	require.NoError(t, err)
	events, tail, err := reopened.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, rootevent.KindRegionDescriptorPublished, events[0].Kind)
	require.Equal(t, uint64(10), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, uint64(11), events[1].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, uint64(12), events[2].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, uint64(3), tail.Index)

	driverState := driver.State()
	require.Equal(t, int64(0), driverState.Checkpoint.LogOffset)
	require.Len(t, driverState.Records, 2)
}

func TestReplicatedStoreInstallBootstrapReplacesState(t *testing.T) {
	store, driver, err := OpenMemory(4)
	require.NoError(t, err)

	_, err = store.Append(
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
	require.NoError(t, store.InstallBootstrap(snapshot, nil))

	current, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, snapshot.State, current)

	events, tail, err := store.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, uint64(99), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, snapshot.State.LastCommitted, tail)

	reopened, err := Open(driver.Config(4))
	require.NoError(t, err)
	current, err = reopened.Current()
	require.NoError(t, err)
	require.Equal(t, snapshot.State, current)

	driverState := driver.State()
	require.Equal(t, snapshot.State, driverState.Checkpoint.Snapshot.State)
	require.Empty(t, driverState.Records)
}

func TestOpenAcceptsDriverConfig(t *testing.T) {
	driver := NewMemoryDriver()
	store, err := Open(Config{Driver: driver, MaxRetainedRecords: 3})
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestSingleNodeDriverAppendAndReopen(t *testing.T) {
	driver, err := NewSingleNodeDriver(1)
	require.NoError(t, err)

	store, err := Open(Config{Driver: driver, MaxRetainedRecords: 4})
	require.NoError(t, err)

	commit, err := store.Append(
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(20, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, commit.Cursor)

	reopened, err := Open(Config{Driver: driver, MaxRetainedRecords: 4})
	require.NoError(t, err)
	current, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, commit.State, current)

	driverState := driver.State()
	require.Len(t, driverState.Records, 2)
	require.Equal(t, uint64(20), driverState.Records[1].Event.RegionDescriptor.Descriptor.RegionID)
}

func TestFixedClusterReplicatesLeaderAppend(t *testing.T) {
	stores, cluster, err := OpenFixedCluster(4, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cluster.LeaderID())

	leader := stores[1]
	commit, err := leader.Append(
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(30, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)

	for _, id := range []uint64{1, 2, 3} {
		driver, err := cluster.Driver(id)
		require.NoError(t, err)
		state := driver.State()
		require.Len(t, state.Records, 2)
		require.Equal(t, uint64(30), state.Records[1].Event.RegionDescriptor.Descriptor.RegionID)

		reopened, err := Open(driver.Config(4))
		require.NoError(t, err)
		current, err := reopened.Current()
		require.NoError(t, err)
		require.Equal(t, commit.State, current)
	}
}

func TestFixedClusterRejectsFollowerAppend(t *testing.T) {
	stores, _, err := OpenFixedCluster(4, 1, 2, 3)
	require.NoError(t, err)

	_, err = stores[2].Append(rootevent.StoreJoined(2, "s2"))
	require.Error(t, err)
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
