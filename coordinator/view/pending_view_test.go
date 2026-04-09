package view

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingViewReplaceAndSnapshotClone(t *testing.T) {
	v := NewPendingView()
	base := testPendingDescriptor(7, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})
	target := base.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.EnsureHash()

	v.Replace(
		map[uint64]rootstate.PendingPeerChange{
			7: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Base: base, Target: target},
		},
		map[uint64]rootstate.PendingRangeChange{
			9: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: 9, LeftRegionID: 9, RightRegionID: 10, BaseParent: base, Left: base, Right: target},
		},
	)

	snapshot := v.Snapshot()
	require.Contains(t, snapshot.PendingPeerChanges, uint64(7))
	require.Contains(t, snapshot.PendingRangeChanges, uint64(9))

	peerChange := snapshot.PendingPeerChanges[7]
	peerChange.Target.StartKey = []byte("mutated")
	snapshot.PendingPeerChanges[7] = peerChange
	rangeChange := snapshot.PendingRangeChanges[9]
	rangeChange.Right.EndKey = []byte("mutated")
	snapshot.PendingRangeChanges[9] = rangeChange

	fresh := v.Snapshot()
	require.Equal(t, []byte("a"), fresh.PendingPeerChanges[7].Target.StartKey)
	require.Equal(t, []byte("z"), fresh.PendingRangeChanges[9].Right.EndKey)
}

func TestPendingViewNilSnapshotIsStable(t *testing.T) {
	var v *PendingView
	snapshot := v.Snapshot()
	require.NotNil(t, snapshot.PendingPeerChanges)
	require.NotNil(t, snapshot.PendingRangeChanges)
	require.Empty(t, snapshot.PendingPeerChanges)
	require.Empty(t, snapshot.PendingRangeChanges)
	v.Replace(nil, nil)
}

func testPendingDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    epoch,
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
