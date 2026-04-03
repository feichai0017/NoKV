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
	_, err = Open(Config{Log: driver.Config(4).Log})
	require.Error(t, err)
	_, err = Open(Config{Checkpoint: driver.Config(4).Checkpoint})
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
