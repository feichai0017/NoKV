package store

import (
	"context"
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/stretchr/testify/require"
)

func TestStoreCheckTxnStatusRoutesThroughPrimaryRegion(t *testing.T) {
	db, localMeta := openStoreDB(t)
	st := NewStore(Config{Scheduler: newTestSchedulerSink(), StoreID: 1, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       713,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 71}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              71,
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
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 71}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	applyTestLockRecord(t, db, []byte("primary-key"), 20, 5)

	resp, err := st.CheckTxnStatus(context.Background(), []byte("primary-key"), 20, 30, 30)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, resp.GetAction())
	require.Nil(t, resp.GetError())

	reader := percolator.NewReader(db)
	lock, err := reader.GetLock([]byte("primary-key"))
	require.NoError(t, err)
	require.Nil(t, lock)
}
