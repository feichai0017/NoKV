package migrate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
)

func TestReadStatusWithRuntimeUsesSeedRegionID(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)
	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 9, PeerID: 109})
	require.NoError(t, err)

	admin := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{{
			Known:        true,
			Hosted:       true,
			LocalPeerId:  109,
			LeaderPeerId: 109,
			Leader:       true,
			AppliedIndex: 7,
			AppliedTerm:  1,
			Region: &pb.RegionMeta{
				Id: 9,
				Peers: []*pb.RegionPeer{
					{StoreId: 1, PeerId: 109},
					{StoreId: 2, PeerId: 209},
				},
			},
		}},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return admin, func() error { return nil }, nil
	}

	status, err := ReadStatusWithConfig(StatusConfig{
		WorkDir:   dir,
		AdminAddr: "leader",
		Dial:      dial,
		Timeout:   time.Second,
	})
	require.NoError(t, err)
	require.Empty(t, status.RuntimeError)
	require.NotNil(t, status.Runtime)
	require.Equal(t, uint64(9), status.Runtime.RegionID)
	require.True(t, status.Runtime.Known)
	require.True(t, status.Runtime.Hosted)
	require.True(t, status.Runtime.Leader)
	require.Equal(t, uint64(109), status.Runtime.LocalPeerID)
	require.Equal(t, uint64(109), status.Runtime.LeaderPeerID)
	require.Equal(t, 2, status.Runtime.MembershipPeers)
	require.Equal(t, uint64(7), status.Runtime.AppliedIndex)
}

func TestReadStatusWithRuntimeCapturesRemoteError(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)
	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 9, PeerID: 109})
	require.NoError(t, err)

	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		return nil, nil, fmt.Errorf("boom")
	}

	status, err := ReadStatusWithConfig(StatusConfig{
		WorkDir:   dir,
		AdminAddr: "leader",
		Dial:      dial,
		Timeout:   time.Second,
	})
	require.NoError(t, err)
	require.Nil(t, status.Runtime)
	require.Contains(t, status.RuntimeError, "dial admin leader")
}
