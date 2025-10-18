package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/store"
)

type noopTransport struct{}

func (noopTransport) Send(myraft.Message) {}

func TestStorePeerLifecycle(t *testing.T) {
	router := store.NewRouter()
	rs := store.NewStore(router)

	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &manifest.RegionMeta{
			ID:       100,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	require.NotNil(t, peer)

	metas := rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(100), metas[0].ID)

	require.NoError(t, router.SendTick(peer.ID()))
	require.NoError(t, router.BroadcastTick())
	require.NoError(t, router.BroadcastFlush())

	rs.StopPeer(peer.ID())
	_, ok := router.Peer(peer.ID())
	require.False(t, ok)
}

func TestStoreDuplicatePeer(t *testing.T) {
	rs := store.NewStore(nil)
	cfg := &raftstore.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)

	defer rs.StopPeer(peer.ID())

	_, err = rs.StartPeer(cfg, nil)
	require.Error(t, err)
}
