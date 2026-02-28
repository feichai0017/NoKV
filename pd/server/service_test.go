package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"
	"github.com/feichai0017/NoKV/pd/tso"
)

type fakeStorage struct {
	updateCalls int
	deleteCalls int
	saveCalls   int
	updateErr   error
	deleteErr   error
	saveErr     error
	lastID      uint64
	lastTS      uint64
}

func (f *fakeStorage) Load() (pdstorage.Snapshot, error) {
	return pdstorage.Snapshot{}, nil
}

func (f *fakeStorage) SaveRegion(meta manifest.RegionMeta) error {
	f.updateCalls++
	if f.updateErr != nil {
		return f.updateErr
	}
	if meta.ID == 0 {
		return errors.New("invalid region id")
	}
	return nil
}

func (f *fakeStorage) DeleteRegion(regionID uint64) error {
	f.deleteCalls++
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if regionID == 0 {
		return errors.New("invalid region id")
	}
	return nil
}

func (f *fakeStorage) SaveAllocatorState(idCurrent, tsCurrent uint64) error {
	f.saveCalls++
	f.lastID = idCurrent
	f.lastTS = tsCurrent
	return f.saveErr
}

func (f *fakeStorage) Close() error {
	return nil
}

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

func TestServiceRemoveRegion(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	_, err := svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               11,
			StartKey:         []byte("a"),
			EndKey:           []byte("z"),
			EpochVersion:     1,
			EpochConfVersion: 1,
		},
	})
	require.NoError(t, err)

	resp, err := svc.RemoveRegion(context.Background(), &pb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, resp.GetRemoved())

	getResp, err := svc.GetRegionByKey(context.Background(), &pb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	resp, err = svc.RemoveRegion(context.Background(), &pb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.False(t, resp.GetRemoved())
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

	_, err = svc.RemoveRegion(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), &pb.RemoveRegionRequest{RegionId: 0})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServiceStoreHeartbeatReturnsLeaderTransferHint(t *testing.T) {
	svc := NewService(core.NewCluster(), nil, nil)
	_, err := svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               100,
			StartKey:         []byte(""),
			EndKey:           []byte("z"),
			EpochVersion:     1,
			EpochConfVersion: 1,
			Peers: []*pb.RegionPeer{
				{StoreId: 1, PeerId: 101},
				{StoreId: 2, PeerId: 201},
			},
		},
	})
	require.NoError(t, err)

	_, err = svc.StoreHeartbeat(context.Background(), &pb.StoreHeartbeatRequest{
		StoreId:   2,
		LeaderNum: 1,
		RegionNum: 1,
	})
	require.NoError(t, err)

	resp, err := svc.StoreHeartbeat(context.Background(), &pb.StoreHeartbeatRequest{
		StoreId:   1,
		LeaderNum: 10,
		RegionNum: 1,
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Len(t, resp.GetOperations(), 1)
	op := resp.GetOperations()[0]
	require.Equal(t, pb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER, op.GetType())
	require.Equal(t, uint64(100), op.GetRegionId())
	require.Equal(t, uint64(101), op.GetSourcePeerId())
	require.Equal(t, uint64(201), op.GetTargetPeerId())
}

func TestServicePersistsRegionCatalog(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	_, err := svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               42,
			StartKey:         []byte("a"),
			EndKey:           []byte("z"),
			EpochVersion:     1,
			EpochConfVersion: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, store.updateCalls)

	_, err = svc.RemoveRegion(context.Background(), &pb.RemoveRegionRequest{RegionId: 42})
	require.NoError(t, err)
	require.Equal(t, 1, store.deleteCalls)
}

func TestServiceRegionCatalogPersistenceErrors(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{updateErr: errors.New("persist update failed")}
	svc.SetStorage(store)

	_, err := svc.RegionHeartbeat(context.Background(), &pb.RegionHeartbeatRequest{
		Region: &pb.RegionMeta{
			Id:               8,
			StartKey:         []byte("a"),
			EndKey:           []byte("m"),
			EpochVersion:     1,
			EpochConfVersion: 1,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	store.updateErr = nil
	store.deleteErr = errors.New("persist delete failed")
	_, err = svc.RemoveRegion(context.Background(), &pb.RemoveRegionRequest{RegionId: 8})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestServicePersistsAllocatorState(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(10), tso.NewAllocator(100))
	store := &fakeStorage{}
	svc.SetStorage(store)

	idResp, err := svc.AllocID(context.Background(), &pb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(11), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)

	tsResp, err := svc.Tso(context.Background(), &pb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(11), store.lastID)
	require.Equal(t, uint64(102), store.lastTS)
}

func TestServiceAllocatorStatePersistenceError(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{saveErr: errors.New("persist failed")}
	svc.SetStorage(store)

	_, err := svc.AllocID(context.Background(), &pb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
}
