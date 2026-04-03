package materialize_test

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestSnapshotDescriptorEventsSorted(t *testing.T) {
	events := rootmaterialize.SnapshotDescriptorEvents(map[uint64]descriptor.Descriptor{
		7: testDescriptor(7, []byte("m"), []byte("z")),
		3: testDescriptor(3, []byte("a"), []byte("m")),
	})
	require.Len(t, events, 2)
	require.Equal(t, rootevent.KindRegionDescriptorPublished, events[0].Kind)
	require.Equal(t, uint64(3), events[0].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, uint64(7), events[1].RegionDescriptor.Descriptor.RegionID)
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
