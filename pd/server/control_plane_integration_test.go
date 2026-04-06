package server_test

import (
	"context"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	pdtestcluster "github.com/feichai0017/NoKV/pd/testcluster"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestControlPlaneFollowerRefreshesFromReplicatedRoot(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followerID := cluster.FollowerIDs(leaderID)[0]
	follower := cluster.Services[followerID]

	require.NoError(t, publishControlPlaneDescriptorEvent(leader, controlPlaneDescriptor(191, []byte("a"), []byte("z"))))

	resp, err := follower.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, resp.GetNotFound())

	require.Eventually(t, func() bool {
		if err := follower.RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := follower.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 191
	}, 8*time.Second, 50*time.Millisecond)
}

func TestControlPlaneWatchedFollowerReloadsFromTailSubscription(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followerID := cluster.FollowerIDs(leaderID)[0]
	subscription := cluster.SubscribeTail(followerID, rootstorage.TailToken{})
	require.NotNil(t, subscription)

	require.NoError(t, publishControlPlaneDescriptorEvent(leader, controlPlaneDescriptor(192, []byte("a"), []byte("z"))))
	cluster.WaitReloaded(followerID, subscription, []byte("m"), 192)
}

func TestControlPlaneReplicatedRootPropagatesToBothFollowers(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followers := cluster.FollowerIDs(leaderID)
	require.Len(t, followers, 2)
	watchedFollowerID := followers[0]
	refreshingFollowerID := followers[1]

	subscription := cluster.SubscribeTail(watchedFollowerID, rootstorage.TailToken{})
	require.NotNil(t, subscription)

	require.NoError(t, publishControlPlaneDescriptorEvent(leader, controlPlaneDescriptor(193, []byte("m"), []byte("z"))))
	cluster.WaitReloaded(watchedFollowerID, subscription, []byte("x"), 193)

	require.Eventually(t, func() bool {
		if err := cluster.Services[refreshingFollowerID].RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := cluster.Services[refreshingFollowerID].GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("x")})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 193
	}, 8*time.Second, 50*time.Millisecond)
}

func publishControlPlaneDescriptorEvent(svc *pdserver.Service, desc descriptor.Descriptor) error {
	_, err := svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionBootstrapped(desc)),
	})
	return err
}

func controlPlaneDescriptor(id uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
