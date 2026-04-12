package integration_test

import (
	"context"
	"testing"
	"time"

	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

func TestReplicatedRootAllocatorFencesPropagateToFollower(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followerID := cluster.FollowerIDs(leaderID)[0]
	subscription := cluster.SubscribeTail(followerID, rootstorage.TailToken{})
	require.NotNil(t, subscription)

	allocResp, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 4})
	require.NoError(t, err)
	require.Equal(t, uint64(4), allocResp.GetCount())
	lastID := allocResp.GetFirstId() + allocResp.GetCount() - 1

	tsoResp, err := leader.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(3), tsoResp.GetCount())
	lastTS := tsoResp.GetTimestamp() + tsoResp.GetCount() - 1

	require.Eventually(t, func() bool {
		advance, err := subscription.Next(context.Background(), 500*time.Millisecond)
		if err != nil {
			return false
		}
		switch advance.CatchUpAction() {
		case rootstorage.TailCatchUpRefreshState, rootstorage.TailCatchUpInstallBootstrap, rootstorage.TailCatchUpAcknowledgeWindow:
			subscription.Acknowledge(advance)
		default:
			return false
		}
		snapshot, err := cluster.RootStores[followerID].Load()
		if err != nil {
			return false
		}
		return snapshot.Allocator.IDCurrent >= lastID && snapshot.Allocator.TSCurrent >= lastTS
	}, 8*time.Second, 50*time.Millisecond)
}

func TestReplicatedRootRemoveRegionPropagatesToFollowers(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followers := cluster.FollowerIDs(leaderID)
	watchedFollowerID := followers[0]
	refreshingFollowerID := followers[1]
	subscription := cluster.SubscribeTail(watchedFollowerID, rootstorage.TailToken{})
	require.NotNil(t, subscription)

	require.NoError(t, publishControlPlaneDescriptorEvent(leader, controlPlaneDescriptor(211, []byte("a"), []byte("z"))))
	cluster.WaitReloaded(watchedFollowerID, subscription, []byte("m"), 211)
	require.Eventually(t, func() bool {
		if err := cluster.Services[refreshingFollowerID].RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := cluster.Services[refreshingFollowerID].GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 211
	}, 8*time.Second, 50*time.Millisecond)

	_, err := leader.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 211})
	require.NoError(t, err)

	cluster.WaitNotFoundReloaded(watchedFollowerID, subscription, []byte("m"))
	require.Eventually(t, func() bool {
		if err := cluster.Services[refreshingFollowerID].RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := cluster.Services[refreshingFollowerID].GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
		return err == nil && resp.GetNotFound()
	}, 8*time.Second, 50*time.Millisecond)
}

func TestReplicatedRootFollowerReadCanBeStaleUntilReload(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followerID := cluster.FollowerIDs(leaderID)[0]
	follower := cluster.Services[followerID]

	require.NoError(t, publishControlPlaneDescriptorEvent(leader, controlPlaneDescriptor(212, []byte("a"), []byte("z"))))
	require.Eventually(t, func() bool {
		if err := follower.RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := follower.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 212
	}, 8*time.Second, 50*time.Millisecond)

	_, err := leader.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 212})
	require.NoError(t, err)

	staleResp, err := follower.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, staleResp.GetNotFound())
	require.Equal(t, uint64(212), staleResp.GetRegionDescriptor().GetRegionId())

	require.Eventually(t, func() bool {
		if err := follower.RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := follower.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
		return err == nil && resp.GetNotFound()
	}, 8*time.Second, 50*time.Millisecond)
}
