package descriptor

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescriptorEqual(t *testing.T) {
	base := Descriptor{
		RegionID:  7,
		StartKey:  []byte("a"),
		EndKey:    []byte("m"),
		Epoch:     metaregion.Epoch{Version: 2, ConfVersion: 3},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}},
		State:     metaregion.ReplicaStateRunning,
		Lineage:   []LineageRef{{RegionID: 6, Epoch: metaregion.Epoch{Version: 1, ConfVersion: 1}, Hash: []byte("h"), Kind: LineageKindSplitParent}},
		RootEpoch: 4,
		Hash:      []byte("hash"),
	}
	require.True(t, base.Equal(base.Clone()))

	changed := base.Clone()
	changed.RootEpoch = 5
	require.False(t, base.Equal(changed))

	changed = base.Clone()
	changed.Lineage[0].Hash = []byte("other")
	require.False(t, base.Equal(changed))
}
