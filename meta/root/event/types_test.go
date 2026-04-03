package event_test

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestRegionSplitCommittedClonesSplitKey(t *testing.T) {
	key := []byte("m")
	left := testDescriptor(1, []byte("a"), []byte("m"))
	right := testDescriptor(2, []byte("m"), []byte("z"))

	event := rootevent.RegionSplitCommitted(1, key, left, right)
	key[0] = 'x'

	require.Equal(t, rootevent.KindRegionSplitCommitted, event.Kind)
	require.NotNil(t, event.RangeSplit)
	require.Equal(t, []byte("m"), event.RangeSplit.SplitKey)
}

func TestCloneEventDetachesPayload(t *testing.T) {
	in := rootevent.RegionDescriptorPublished(testDescriptor(9, []byte("a"), []byte("z")))
	cloned := rootevent.CloneEvent(in)

	in.RegionDescriptor.Descriptor.StartKey[0] = 'x'
	require.Equal(t, byte('a'), cloned.RegionDescriptor.Descriptor.StartKey[0])
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
