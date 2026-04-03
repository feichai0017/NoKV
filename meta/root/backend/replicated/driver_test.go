package replicated

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

type driverStateView interface {
	State() DriverState
}

func TestMemoryDriverContract(t *testing.T) {
	driver := NewMemoryDriver()
	testDriverContract(t, driver)
}

func TestSingleNodeDriverContract(t *testing.T) {
	driver, err := NewSingleNodeDriver(1)
	require.NoError(t, err)
	testDriverContract(t, driver)
}

func testDriverContract(t *testing.T, driver Driver) {
	t.Helper()

	stateView, ok := driver.(driverStateView)
	require.True(t, ok)

	log := driver.Log()
	checkpt := driver.CheckpointStore()
	installer := driver.BootstrapInstaller()

	loaded, err := checkpt.Load()
	require.NoError(t, err)
	require.Equal(t, rootstate.State{}, loaded.Snapshot.State)
	require.Empty(t, loaded.Snapshot.Descriptors)
	require.Equal(t, int64(0), loaded.LogOffset)

	records := []rootstorage.CommittedEvent{
		{
			Cursor: rootstate.Cursor{Term: 1, Index: 1},
			Event:  rootevent.StoreJoined(1, "s1"),
		},
		{
			Cursor: rootstate.Cursor{Term: 1, Index: 2},
			Event:  rootevent.RegionDescriptorPublished(testDriverDescriptor(10, []byte("a"), []byte("z"))),
		},
	}
	logEnd, err := log.Append(records...)
	require.NoError(t, err)
	require.Equal(t, int64(2), logEnd)

	loadedRecords, err := log.Load(0)
	require.NoError(t, err)
	require.Len(t, loadedRecords, 2)
	require.Equal(t, records[1].Cursor, loadedRecords[1].Cursor)
	require.Equal(t, uint64(10), loadedRecords[1].Event.RegionDescriptor.Descriptor.RegionID)

	checkpoint := rootstorage.Checkpoint{
		Snapshot: rootstate.Snapshot{
			State: rootstate.State{
				ClusterEpoch:  1,
				LastCommitted: rootstate.Cursor{Term: 1, Index: 2},
				IDFence:       8,
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				10: testDriverDescriptor(10, []byte("a"), []byte("z")),
			},
		},
		LogOffset: 1,
	}
	require.NoError(t, checkpt.Save(checkpoint))
	loaded, err = checkpt.Load()
	require.NoError(t, err)
	require.Equal(t, checkpoint.Snapshot.State, loaded.Snapshot.State)
	require.Equal(t, int64(1), loaded.LogOffset)

	require.NoError(t, log.Compact(loadedRecords[1:]))
	loadedRecords, err = log.Load(0)
	require.NoError(t, err)
	require.Len(t, loadedRecords, 1)
	require.Equal(t, uint64(10), loadedRecords[0].Event.RegionDescriptor.Descriptor.RegionID)

	replaced := rootstorage.Checkpoint{
		Snapshot: rootstate.Snapshot{
			State: rootstate.State{
				ClusterEpoch:  9,
				LastCommitted: rootstate.Cursor{Term: 3, Index: 7},
				TSOFence:      99,
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				42: testDriverDescriptor(42, []byte("m"), []byte("z")),
			},
		},
	}
	require.NoError(t, installer.InstallBootstrap(replaced, nil))
	loaded, err = checkpt.Load()
	require.NoError(t, err)
	require.Equal(t, replaced.Snapshot.State, loaded.Snapshot.State)
	loadedRecords, err = log.Load(0)
	require.NoError(t, err)
	require.Empty(t, loadedRecords)

	driverState := stateView.State()
	require.Equal(t, replaced.Snapshot.State, driverState.Checkpoint.Snapshot.State)
	require.Empty(t, driverState.Records)
}

func testDriverDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
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
