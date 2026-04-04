package store

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"testing"
	"time"

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
			State: metaregion.ReplicaStateRunning,
			Peers: []metaregion.Peer{{StoreID: 1, PeerID: 1}},
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
			Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 2, PeerID: 2}}),
		},
	}
	require.NoError(t, rs.handlePeerConfChange(addEvent))

	meta, ok = rs.RegionMetaByID(regionID)
	require.True(t, ok)
	require.Len(t, meta.Peers, 2)
	require.Equal(t, uint64(1), meta.Epoch.ConfVersion)
	require.Contains(t, meta.Peers, metaregion.Peer{StoreID: 2, PeerID: 2})

	removeEvent := peer.ConfChangeEvent{
		Peer:       p,
		RegionMeta: p.RegionMeta(),
		ConfChange: raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{
				{Type: raftpb.ConfChangeRemoveNode, NodeID: 1},
			},
			Context: encodeConfChangeContext([]metaregion.Peer{{StoreID: 1, PeerID: 1}}),
		},
	}
	require.NoError(t, rs.handlePeerConfChange(removeEvent))

	_, ok = rs.RegionMetaByID(regionID)
	require.False(t, ok)
	_, hosted := rs.Peer(1)
	require.False(t, hosted)
}

func TestAddPeerPublishesPlannedTarget(t *testing.T) {
	db, localMeta := openStoreDB(t)
	sink := newTestSchedulerSink()
	rs := NewStore(Config{Scheduler: sink, StoreID: 1})
	t.Cleanup(func() { rs.Close() })

	region := &localmeta.RegionMeta{
		ID:       202,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
		State:    metaregion.ReplicaStateRunning,
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { rs.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())
	sink.ResetHistory()

	require.NoError(t, rs.AddPeer(region.ID, metaregion.Peer{StoreID: 2, PeerID: 2}))
	history := sink.EventHistory()
	require.Len(t, history, 1)
	require.Equal(t, "root", history[0].kind)
	require.Equal(t, rootevent.KindPeerAdditionPlanned, history[0].rootKind)

	require.Eventually(t, func() bool {
		meta, ok := rs.RegionMetaByID(region.ID)
		return ok && peerIndexByID(meta.Peers, 2) >= 0
	}, time.Second, 10*time.Millisecond)

	sink.ResetHistory()
	require.NoError(t, rs.AddPeer(region.ID, metaregion.Peer{StoreID: 2, PeerID: 2}))
	time.Sleep(50 * time.Millisecond)
	for _, ev := range sink.EventHistory() {
		require.NotEqual(t, rootevent.KindPeerAdditionPlanned, ev.rootKind)
	}
}
