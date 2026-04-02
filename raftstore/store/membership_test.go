package store

import (
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"testing"

	"github.com/stretchr/testify/require"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestHandlePeerConfChangeUpdatesRegionMeta(t *testing.T) {
	rs := NewStore(Config{})

	const regionID = uint64(101)

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:    regionID,
			State: localmeta.RegionStateRunning,
			Peers: []localmeta.PeerMeta{{StoreID: 1, PeerID: 1}},
		},
	}

	p, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	defer rs.StopPeer(p.ID())

	meta, ok := rs.RegionMetaByID(regionID)
	require.True(t, ok)
	require.Len(t, meta.Peers, 1)
	require.Equal(t, uint64(0), meta.Epoch.ConfVersion)

	addEvent := peer.ConfChangeEvent{
		Peer:       p,
		RegionMeta: p.RegionMeta(),
		ConfChange: raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{
				{Type: raftpb.ConfChangeAddNode, NodeID: 2},
			},
			Context: encodeConfChangeContext([]localmeta.PeerMeta{{StoreID: 2, PeerID: 2}}),
		},
	}
	require.NoError(t, rs.handlePeerConfChange(addEvent))

	meta, ok = rs.RegionMetaByID(regionID)
	require.True(t, ok)
	require.Len(t, meta.Peers, 2)
	require.Equal(t, uint64(1), meta.Epoch.ConfVersion)
	require.Contains(t, meta.Peers, localmeta.PeerMeta{StoreID: 2, PeerID: 2})

	removeEvent := peer.ConfChangeEvent{
		Peer:       p,
		RegionMeta: p.RegionMeta(),
		ConfChange: raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{
				{Type: raftpb.ConfChangeRemoveNode, NodeID: 1},
			},
			Context: encodeConfChangeContext([]localmeta.PeerMeta{{StoreID: 1, PeerID: 1}}),
		},
	}
	require.NoError(t, rs.handlePeerConfChange(removeEvent))

	_, ok = rs.RegionMetaByID(regionID)
	require.False(t, ok)
	_, hosted := rs.Peer(1)
	require.False(t, hosted)
}
