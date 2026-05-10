package client

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/meta/topology"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestBuildReadContextTargetsFollowerAndCarriesPolicy(t *testing.T) {
	region := regionSnapshot{
		desc: topology.Descriptor{
			RegionID: 7,
			Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 3},
			Peers: []metaregion.Peer{
				{StoreID: 1, PeerID: 101},
				{StoreID: 2, PeerID: 102},
			},
		},
		leader: 1,
	}
	ctx, err := buildReadContext(region, 2, ReadOptions{
		Consistency:       kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE,
		Preference:        kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER,
		MaxStaleReadIndex: 11,
		MaxStaleReadMS:    12,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), ctx.GetRegionId())
	require.Equal(t, uint64(2), ctx.GetPeer().GetStoreId())
	require.Equal(t, uint64(102), ctx.GetPeer().GetPeerId())
	require.Equal(t, kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE, ctx.GetReadConsistency())
	require.Equal(t, kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER, ctx.GetReadPreference())
	require.Equal(t, uint64(11), ctx.GetMaxStaleReadIndex())
	require.Equal(t, uint64(12), ctx.GetMaxStaleReadMs())
}

func TestFollowerStoreIDSkipsLeader(t *testing.T) {
	region := regionSnapshot{
		desc: topology.Descriptor{
			RegionID: 7,
			Peers: []metaregion.Peer{
				{StoreID: 1, PeerID: 101},
				{StoreID: 2, PeerID: 102},
			},
		},
		leader: 1,
	}
	require.Equal(t, uint64(2), followerStoreID(region))
}
