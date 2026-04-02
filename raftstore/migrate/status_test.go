package migrate

import (
	"context"
	"fmt"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadStatusWithRuntimeUsesSeedRegionID(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)
	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 9, PeerID: 109})
	require.NoError(t, err)

	admin := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{{
			Known:        true,
			Hosted:       true,
			LocalPeerId:  109,
			LeaderPeerId: 109,
			Leader:       true,
			AppliedIndex: 7,
			AppliedTerm:  1,
			Region: &metapb.RegionDescriptor{
				RegionId: 9,
				Peers: []*metapb.RegionPeer{
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

func TestBuildReportIncludesClusterSummary(t *testing.T) {
	dir := prepareStandaloneWorkdir(t)
	_, err := Init(InitConfig{WorkDir: dir, StoreID: 1, RegionID: 9, PeerID: 109})
	require.NoError(t, err)

	admin := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{{
			Known:        true,
			Hosted:       true,
			LocalPeerId:  109,
			LeaderPeerId: 209,
			Leader:       false,
			AppliedIndex: 7,
			AppliedTerm:  1,
			Region: &metapb.RegionDescriptor{
				RegionId: 9,
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 109},
					{StoreId: 2, PeerId: 209},
					{StoreId: 3, PeerId: 309},
				},
			},
		}},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return admin, func() error { return nil }, nil
	}

	report, err := BuildReportWithConfig(StatusConfig{
		WorkDir:   dir,
		AdminAddr: "leader",
		Dial:      dial,
		Timeout:   time.Second,
	})
	require.NoError(t, err)
	require.NotNil(t, report.Cluster)
	require.Equal(t, "single-admin-endpoint", report.Cluster.Source)
	require.Equal(t, "leader", report.Cluster.AdminAddr)
	require.Equal(t, uint64(9), report.Cluster.RegionID)
	require.Equal(t, uint64(209), report.Cluster.LeaderPeerID)
	require.Equal(t, uint64(2), report.Cluster.LeaderStoreID)
	require.Equal(t, 3, report.Cluster.MembershipPeers)
	require.Len(t, report.Cluster.Membership, 3)
	require.Equal(t, uint64(3), report.Cluster.Membership[2].StoreID)
	require.Equal(t, uint64(309), report.Cluster.Membership[2].PeerID)
}
