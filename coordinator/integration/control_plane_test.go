package integration_test

import (
	"context"
	"testing"
	"time"

	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControlPlaneFollowerRefreshesFromReplicatedRoot(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followerID := cluster.FollowerIDs(leaderID)[0]
	follower := cluster.Services[followerID]

	require.NoError(t, publishControlPlaneDescriptorEvent(leader, controlPlaneDescriptor(191, []byte("a"), []byte("z"))))

	resp, err := follower.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, resp.GetNotFound())

	require.Eventually(t, func() bool {
		if err := follower.RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := follower.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
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
		resp, err := cluster.Services[refreshingFollowerID].GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("x")})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 193
	}, 8*time.Second, 50*time.Millisecond)
}

func TestControlPlaneFollowerRejectsLeaderOnlyWrites(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	followerID := cluster.FollowerIDs(leaderID)[0]
	follower := cluster.Services[followerID]

	_, err := follower.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionBootstrapped(controlPlaneDescriptor(194, []byte("a"), []byte("b")))),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "coordinator not leader")

	_, err = leader.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionBootstrapped(controlPlaneDescriptor(195, []byte("b"), []byte("c")))),
	})
	require.NoError(t, err)
}

func TestControlPlaneLeaderOnlyAllocatorsRejectFollowerWrites(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID, leader := cluster.LeaderService()
	follower := cluster.Services[cluster.FollowerIDs(leaderID)[0]]

	alloc, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(3), alloc.GetCount())
	require.NotZero(t, alloc.GetFirstId())

	nextAlloc, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, alloc.GetFirstId()+alloc.GetCount(), nextAlloc.GetFirstId())

	ts, err := leader.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(2), ts.GetCount())
	require.NotZero(t, ts.GetTimestamp())

	_, err = follower.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "coordinator not leader")

	_, err = follower.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "coordinator not leader")
}

func publishControlPlaneDescriptorEvent(svc *coordserver.Service, desc descriptor.Descriptor) error {
	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
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
