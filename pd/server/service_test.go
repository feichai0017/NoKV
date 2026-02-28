package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
	"github.com/feichai0017/NoKV/pd/tso"
)

func TestServiceStoreHeartbeatAndGetRegionByKey(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))

	storeResp, err := svc.StoreHeartbeat(context.Background(), &pb.StoreHeartbeatRequest{
		StoreId:   1,
		RegionNum: 3,
		LeaderNum: 1,
		Capacity:  1000,
		Available: 800,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())

	_, err = svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               11,
			StartKey:         []byte(""),
			EndKey:           []byte("m"),
			EpochVersion:     1,
			EpochConfVersion: 1,
			Peers: []*pb.RegionPeer{
				{StoreId: 1, PeerId: 101},
			},
		},
	})
	require.NoError(t, err)

	getResp, err := svc.GetRegionByKey(context.Background(), &pb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.NotNil(t, getResp.GetRegion())
	require.Equal(t, uint64(11), getResp.GetRegion().GetId())
}

func TestServiceRegionHeartbeatRejectsStaleAndOverlap(t *testing.T) {
	svc := NewService(core.NewCluster(), nil, nil)
	_, err := svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               1,
			StartKey:         []byte("a"),
			EndKey:           []byte("m"),
			EpochVersion:     2,
			EpochConfVersion: 2,
		},
	})
	require.NoError(t, err)

	_, err = svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               1,
			StartKey:         []byte("a"),
			EndKey:           []byte("m"),
			EpochVersion:     1,
			EpochConfVersion: 2,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               2,
			StartKey:         []byte("l"),
			EndKey:           []byte("z"),
			EpochVersion:     1,
			EpochConfVersion: 1,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestServiceAllocIDAndTSO(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(100), tso.NewAllocator(500))

	idResp, err := svc.AllocID(context.Background(), &pb.AllocIDRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), idResp.GetFirstId())
	require.Equal(t, uint64(3), idResp.GetCount())

	tsResp, err := svc.Tso(context.Background(), &pb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(500), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
}

func TestServiceRequestValidation(t *testing.T) {
	svc := NewService(nil, nil, nil)

	_, err := svc.StoreHeartbeat(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.AllocID(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.Tso(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
