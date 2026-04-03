package replicated

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"testing"
)

type memCheckpoint struct {
	checkpoint rootstorage.Checkpoint
}

func (m *memCheckpoint) Load() (rootstorage.Checkpoint, error) {
	return rootstorage.CloneCheckpoint(m.checkpoint), nil
}

func (m *memCheckpoint) Save(checkpoint rootstorage.Checkpoint) error {
	m.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	return nil
}

type memLog struct{ records []rootstorage.CommittedEvent }

func (m *memLog) Load(offset int64) ([]rootstorage.CommittedEvent, error) {
	if offset <= 0 || int(offset) > len(m.records) {
		return rootstorage.CloneCommittedEvents(m.records), nil
	}
	return rootstorage.CloneCommittedEvents(m.records[int(offset):]), nil
}

func (m *memLog) Append(records ...rootstorage.CommittedEvent) (int64, error) {
	m.records = append(m.records, rootstorage.CloneCommittedEvents(records)...)
	return int64(len(m.records)), nil
}

func (m *memLog) Compact(records []rootstorage.CommittedEvent) error {
	m.records = rootstorage.CloneCommittedEvents(records)
	return nil
}

func (m *memLog) Size() (int64, error) { return int64(len(m.records)), nil }

func TestReplicatedStoreAppendAndReopen(t *testing.T) {
	cp := &memCheckpoint{checkpoint: rootstorage.Checkpoint{Snapshot: rootstate.Snapshot{Descriptors: map[uint64]descriptor.Descriptor{}}}}
	log := &memLog{}
	store, err := Open(Config{Log: log, Checkpoint: cp, MaxRetainedRecords: 4})
	require.NoError(t, err)

	commit, err := store.Append(
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, commit.Cursor)
	require.Equal(t, uint64(1), commit.State.MembershipEpoch)
	require.Equal(t, uint64(1), commit.State.ClusterEpoch)

	reopened, err := Open(Config{Log: log, Checkpoint: cp, MaxRetainedRecords: 4})
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
	_, err = Open(Config{Log: &memLog{}})
	require.Error(t, err)
	_, err = Open(Config{Checkpoint: &memCheckpoint{checkpoint: rootstorage.Checkpoint{Snapshot: rootstate.Snapshot{Descriptors: map[uint64]descriptor.Descriptor{}}}}})
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
