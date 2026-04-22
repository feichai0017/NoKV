package localmeta

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
)

func TestRegionDescriptorHelpers(t *testing.T) {
	meta := RegionMeta{
		ID:       88,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: metaregion.Epoch{
			Version:     7,
			ConfVersion: 9,
		},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 202},
		},
		State: RegionStateRunning,
	}

	desc := Descriptor(meta, 55)
	require.Equal(t, meta.ID, desc.RegionID)
	require.Equal(t, uint64(55), desc.RootEpoch)
	require.Equal(t, meta.Peers, desc.Peers)
	require.NotEmpty(t, desc.Hash)

	fromDesc := FromDescriptor(desc)
	require.Equal(t, meta, fromDesc)

	desc.StartKey[0] = 'z'
	require.Equal(t, []byte("a"), meta.StartKey)

	pbDesc := DescriptorToProto(meta)
	require.Equal(t, meta.ID, pbDesc.GetRegionId())

	fromPB, err := FromDescriptorProto(pbDesc)
	require.NoError(t, err)
	require.Equal(t, meta, fromPB)

	_, err = FromDescriptorProto(nil)
	require.ErrorContains(t, err, "region descriptor is nil")

	pbMeta := regionMetaToPB(meta)
	require.Equal(t, meta.ID, pbMeta.GetRegionId())
	require.Equal(t, metapb.RegionReplicaState_REGION_REPLICA_STATE_RUNNING, pbMeta.GetState())
	require.Len(t, pbMeta.GetPeers(), 2)

	roundTrip := regionMetaFromPB(&metapb.LocalRegionMeta{
		RegionId: meta.ID,
		StartKey: meta.StartKey,
		EndKey:   meta.EndKey,
		Epoch: &metapb.RegionEpoch{
			Version:     meta.Epoch.Version,
			ConfVersion: meta.Epoch.ConfVersion,
		},
		State: metapb.RegionReplicaState_REGION_REPLICA_STATE_REMOVING,
		Peers: []*metapb.RegionPeer{
			nil,
			{StoreId: 3, PeerId: 303},
		},
	})
	require.Equal(t, RegionStateRemoving, roundTrip.State)
	require.Equal(t, []metaregion.Peer{{StoreID: 3, PeerID: 303}}, roundTrip.Peers)

	require.Equal(t, RegionMeta{}, regionMetaFromPB(nil))
	require.Equal(t, metapb.RegionReplicaState_REGION_REPLICA_STATE_UNSPECIFIED, regionStateToPB(metaregion.ReplicaState(99)))
	require.Equal(t, RegionStateNew, regionStateFromPB(metapb.RegionReplicaState_REGION_REPLICA_STATE_UNSPECIFIED))
	require.Equal(t, RegionStateNew, regionStateFromPB(metapb.RegionReplicaState_REGION_REPLICA_STATE_NEW))
	require.Equal(t, RegionStateRunning, regionStateFromPB(metapb.RegionReplicaState_REGION_REPLICA_STATE_RUNNING))
	require.Equal(t, RegionStateRemoving, regionStateFromPB(metapb.RegionReplicaState_REGION_REPLICA_STATE_REMOVING))
	require.Equal(t, RegionStateTombstone, regionStateFromPB(metapb.RegionReplicaState_REGION_REPLICA_STATE_TOMBSTONE))
}
