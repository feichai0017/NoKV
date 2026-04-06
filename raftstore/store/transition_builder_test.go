package store

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestTransitionBuilderPeerChangeTarget(t *testing.T) {
	var nilStore *Store
	_, err := nilStore.buildPeerChangeTarget(1, raftpb.ConfChangeV2{})
	require.Error(t, err)

	rs := NewStore(Config{})
	require.NoError(t, rs.applyRegionMetaSilent(localmeta.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
		State:    metaregion.ReplicaStateRunning,
	}))

	_, err = rs.buildPeerChangeTarget(0, raftpb.ConfChangeV2{})
	require.Error(t, err)
	_, err = rs.buildPeerChangeTarget(99, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 2}},
	})
	require.Error(t, err)

	add := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 2}},
		Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 2, PeerID: 2}}),
	}
	target, err := rs.buildPeerChangeTarget(10, add)
	require.NoError(t, err)
	require.Equal(t, uint64(10), target.RegionID)
	require.Equal(t, "peer change", target.Action)
	require.Equal(t, rootevent.KindPeerAdditionPlanned, target.Event.Kind)
	require.NotNil(t, target.Proposal.ConfChange)
	require.Nil(t, target.Proposal.Admin)

	require.NoError(t, rs.applyRegionMetaSilent(localmeta.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 2},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}, {StoreID: 2, PeerID: 2}},
		State:    metaregion.ReplicaStateRunning,
	}))
	target, err = rs.buildPeerChangeTarget(10, add)
	require.NoError(t, err)
	require.True(t, target.Noop)

	remove := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}},
		Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 2, PeerID: 2}}),
	}
	target, err = rs.buildPeerChangeTarget(10, remove)
	require.NoError(t, err)
	require.Equal(t, rootevent.KindPeerRemovalPlanned, target.Event.Kind)
}

func TestTransitionBuilderSplitPlanTargetAndNoop(t *testing.T) {
	rs := NewStore(Config{PeerBuilder: testPeerBuilder(1)})
	parent := localmeta.RegionMeta{
		ID:       20,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 11}},
		State:    metaregion.ReplicaStateRunning,
	}
	require.NoError(t, rs.applyRegionMetaSilent(parent))

	child := localmeta.RegionMeta{
		ID:     21,
		EndKey: []byte("z"),
		Peers:  []metaregion.Peer{{StoreID: 1, PeerID: 12}},
	}
	plan, err := rs.buildSplitPlan(parent.ID, child, []byte("m"))
	require.NoError(t, err)
	require.Equal(t, []byte("m"), plan.parent.EndKey)
	require.Equal(t, uint64(2), plan.parent.Epoch.Version)
	require.Equal(t, []byte("m"), plan.child.StartKey)
	require.Equal(t, metaregion.ReplicaStateRunning, plan.child.State)
	require.Equal(t, parent.ID, plan.parentDesc.RegionID)
	require.Equal(t, child.ID, plan.childDesc.RegionID)

	target, err := rs.buildSplitTarget(parent.ID, child, []byte("m"))
	require.NoError(t, err)
	require.Equal(t, rootevent.KindRegionSplitPlanned, target.Event.Kind)
	require.NotNil(t, target.Proposal.Admin)
	require.Equal(t, raftcmdpb.AdminCommand_SPLIT, target.Proposal.Admin.Type)

	require.NoError(t, rs.applyRegionMetaSilent(plan.parent))
	require.NoError(t, rs.applyRegionMetaSilent(plan.child))
	target, err = rs.buildSplitTarget(parent.ID, child, []byte("m"))
	require.NoError(t, err)
	require.True(t, target.Noop)
}

func TestTransitionBuilderMergePlanAndChildConfig(t *testing.T) {
	noBuilder := NewStore(Config{})
	_, _, err := noBuilder.buildChildPeerConfig(localmeta.RegionMeta{})
	require.Error(t, err)

	rs := NewStore(Config{PeerBuilder: testPeerBuilder(1)})
	targetMeta := localmeta.RegionMeta{
		ID:       30,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 31}},
		State:    metaregion.ReplicaStateRunning,
	}
	sourceMeta := localmeta.RegionMeta{
		ID:       31,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 32}},
		State:    metaregion.ReplicaStateRunning,
	}
	require.NoError(t, rs.applyRegionMetaSilent(targetMeta))
	require.NoError(t, rs.applyRegionMetaSilent(sourceMeta))

	plan, err := rs.buildMergePlan(targetMeta.ID, sourceMeta.ID)
	require.NoError(t, err)
	require.Equal(t, sourceMeta.ID, plan.leftID)
	require.Equal(t, targetMeta.ID, plan.rightID)
	require.Equal(t, []byte("z"), plan.mergedDesc.EndKey)
	require.Equal(t, uint64(2), plan.target.Epoch.Version)

	target, err := rs.buildMergeTarget(targetMeta.ID, sourceMeta.ID)
	require.NoError(t, err)
	require.Equal(t, rootevent.KindRegionMergePlanned, target.Event.Kind)
	require.NotNil(t, target.Proposal.Admin)
	require.Equal(t, raftcmdpb.AdminCommand_MERGE, target.Proposal.Admin.Type)

	cfg, peers, err := rs.buildChildPeerConfig(sourceMeta)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Len(t, peers, 1)
	require.Equal(t, uint64(32), peers[0].ID)
}

func TestTransitionBuilderConfChangeContextRoundTrip(t *testing.T) {
	peers := []metaregion.Peer{{StoreID: 2, PeerID: 22}, {StoreID: 3, PeerID: 33}}
	encoded := encodeConfChangeContext(peers)
	decoded, err := decodeConfChangeContext(encoded)
	require.NoError(t, err)
	require.Equal(t, peers, decoded)

	_, err = decodeConfChangeContext([]byte{0x80})
	require.Error(t, err)
}
