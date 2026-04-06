package store

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestTransitionOutcomeAppliedPeerChangeEvent(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       51,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 2},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}, {StoreID: 2, PeerID: 2}},
		State:    metaregion.ReplicaStateRunning,
	}

	add, ok := appliedPeerChangeEvent(meta, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 2}},
		Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 2, PeerID: 2}}),
	})
	require.True(t, ok)
	require.Equal(t, rootevent.KindPeerAdded, add.Kind)

	remove, ok := appliedPeerChangeEvent(meta, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}},
		Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 2, PeerID: 2}}),
	})
	require.True(t, ok)
	require.Equal(t, rootevent.KindPeerRemoved, remove.Kind)

	_, ok = appliedPeerChangeEvent(localmeta.RegionMeta{}, raftpb.ConfChangeV2{})
	require.False(t, ok)
	_, ok = appliedPeerChangeEvent(meta, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeUpdateNode, NodeID: 2}},
	})
	require.False(t, ok)
}

func TestTransitionOutcomeCommittedSplitAndMergeEvents(t *testing.T) {
	split := committedSplitEvent(splitPlan{
		originalParent: localmeta.RegionMeta{ID: 61},
		child:          localmeta.RegionMeta{StartKey: []byte("m")},
		parentDesc:     descriptorForOutcome(61, []byte("a"), []byte("m")),
		childDesc:      descriptorForOutcome(62, []byte("m"), []byte("z")),
	})
	require.Equal(t, rootevent.KindRegionSplitCommitted, split.Kind)
	require.Equal(t, uint64(61), split.RangeSplit.ParentRegionID)
	require.Equal(t, uint64(62), split.RangeSplit.Right.RegionID)

	merge := committedMergeEvent(mergePlan{
		leftID:     70,
		rightID:    71,
		mergedDesc: descriptorForOutcome(70, []byte("a"), []byte("z")),
	})
	require.Equal(t, rootevent.KindRegionMerged, merge.Kind)
	require.Equal(t, uint64(70), merge.RangeMerge.LeftRegionID)
	require.Equal(t, uint64(71), merge.RangeMerge.RightRegionID)
}

func descriptorForOutcome(id uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
