package materialize_test

import (
	"context"
	"errors"
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

type fakeVirtualLog struct {
	checkpoint rootstorage.Checkpoint
	tail       rootstorage.CommittedTail
	loadErr    error
	readErr    error
}

func (f fakeVirtualLog) LoadCheckpoint() (rootstorage.Checkpoint, error) {
	if f.loadErr != nil {
		return rootstorage.Checkpoint{}, f.loadErr
	}
	return f.checkpoint, nil
}

func (f fakeVirtualLog) SaveCheckpoint(rootstorage.Checkpoint) error { return nil }
func (f fakeVirtualLog) ReadCommitted(requestedOffset int64) (rootstorage.CommittedTail, error) {
	if f.readErr != nil {
		return rootstorage.CommittedTail{}, f.readErr
	}
	tail := f.tail
	tail.RequestedOffset = requestedOffset
	return tail, nil
}
func (f fakeVirtualLog) AppendCommitted(context.Context, ...rootstorage.CommittedEvent) (int64, error) {
	return 0, nil
}
func (f fakeVirtualLog) CompactCommitted(rootstorage.CommittedTail) error { return nil }
func (f fakeVirtualLog) InstallBootstrap(rootstorage.ObservedCommitted) error { return nil }
func (f fakeVirtualLog) Size() (int64, error) { return 0, nil }

func TestBootstrapFromObservedReplaysTail(t *testing.T) {
	base := testMaterializeDescriptor(1, []byte("a"), []byte("m"))
	left := testMaterializeDescriptor(1, []byte("a"), []byte("f"))
	right := testMaterializeDescriptor(2, []byte("f"), []byte("m"))
	checkpoint := rootstate.Snapshot{
		State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 1, Index: 1}},
		Descriptors: map[uint64]descriptor.Descriptor{
			base.RegionID: base,
		},
	}

	observed := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: checkpoint, TailOffset: 7},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 7,
			StartOffset:     8,
			EndOffset:       10,
			Records: []rootstorage.CommittedEvent{
				{
					Cursor: rootstate.Cursor{Term: 1, Index: 1},
					Event:  rootevent.IDAllocatorFenced(8),
				},
				{
					Cursor: rootstate.Cursor{Term: 1, Index: 2},
					Event:  rootevent.RegionSplitCommitted(base.RegionID, []byte("f"), left, right),
				},
			},
		},
	}

	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	require.Equal(t, observed.Tail, bootstrap.Tail)
	require.Equal(t, observed.RetainFrom(), bootstrap.RetainFrom)
	require.Equal(t, left, bootstrap.Snapshot.Descriptors[left.RegionID])
	require.Equal(t, right, bootstrap.Snapshot.Descriptors[right.RegionID])
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, bootstrap.Snapshot.State.LastCommitted)
}

func TestLoadBootstrapAndCloneCommittedEvents(t *testing.T) {
	desc := testMaterializeDescriptor(9, []byte("m"), []byte("z"))
	log := fakeVirtualLog{
		checkpoint: rootstorage.Checkpoint{
			Snapshot: rootstate.Snapshot{
				State:       rootstate.State{LastCommitted: rootstate.Cursor{Term: 2, Index: 3}},
				Descriptors: map[uint64]descriptor.Descriptor{},
			},
			TailOffset: 11,
		},
		tail: rootstorage.CommittedTail{
			StartOffset: 11,
			EndOffset:   12,
			Records: []rootstorage.CommittedEvent{
				{
					Cursor: rootstate.Cursor{Term: 2, Index: 4},
					Event:  rootevent.RegionDescriptorPublished(desc),
				},
			},
		},
	}

	bootstrap, err := rootmaterialize.LoadBootstrap(log)
	require.NoError(t, err)
	require.Equal(t, desc, bootstrap.Snapshot.Descriptors[desc.RegionID])

	copied := rootmaterialize.CloneCommittedEvents(log.tail.Records)
	log.tail.Records[0].Event.RegionDescriptor.Descriptor.StartKey[0] = 'x'
	require.Equal(t, byte('m'), copied[0].Event.RegionDescriptor.Descriptor.StartKey[0])
	require.Nil(t, rootmaterialize.CloneCommittedEvents(nil))
}

func TestLoadBootstrapPropagatesReadErrors(t *testing.T) {
	_, err := rootmaterialize.LoadBootstrap(fakeVirtualLog{loadErr: errors.New("load failed")})
	require.ErrorContains(t, err, "load failed")

	_, err = rootmaterialize.LoadBootstrap(fakeVirtualLog{
		checkpoint: rootstorage.Checkpoint{},
		readErr:    errors.New("read failed"),
	})
	require.ErrorContains(t, err, "read failed")
}

func testMaterializeDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
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
