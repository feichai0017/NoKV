package store

import (
	"context"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"testing"

	"github.com/stretchr/testify/require"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type adminTestTransport struct{}

func (adminTestTransport) Send(context.Context, myraft.Message) {}

func adminTestPeerBuilder(storeID uint64) PeerBuilder {
	return func(region localmeta.RegionMeta) (*peer.Config, error) {
		return &peer.Config{
			RaftConfig: myraft.Config{
				ID:              region.Peers[0].PeerID,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
			},
			Transport: adminTestTransport{},
			Apply:     func([]myraft.Entry) error { return nil },
			Region:    localmeta.CloneRegionMetaPtr(&region),
		}, nil
	}
}

func TestStoreLocalSplitStartsChildPeer(t *testing.T) {
	storeID := uint64(1)
	peerBuilder := adminTestPeerBuilder(storeID)
	rs := NewStore(Config{PeerBuilder: peerBuilder, StoreID: storeID})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       1000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []localmeta.PeerMeta{{StoreID: storeID, PeerID: 1}},
	}
	parentCfg, err := peerBuilder(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())

	childMeta := localmeta.RegionMeta{
		ID:       2000,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []localmeta.PeerMeta{{StoreID: storeID, PeerID: 2}},
	}
	childPeer, err := rs.splitRegionLocal(parentMeta.ID, childMeta)
	require.NoError(t, err)
	require.NotNil(t, childPeer)
	defer rs.StopPeer(childPeer.ID())

	parentUpdated, ok := rs.RegionMetaByID(1000)
	require.True(t, ok)
	require.Equal(t, []byte("m"), parentUpdated.EndKey)
	require.Equal(t, uint64(1), parentUpdated.Epoch.Version)

	childUpdated, ok := rs.RegionMetaByID(2000)
	require.True(t, ok)
	require.Equal(t, []byte("m"), childUpdated.StartKey)
	require.Equal(t, []byte("z"), childUpdated.EndKey)
	require.Len(t, childUpdated.Peers, 1)
	require.Equal(t, uint64(2), childUpdated.Peers[0].PeerID)
}
