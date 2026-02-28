package adapter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

type fakePDClient struct {
	storeReqs  []*pb.StoreHeartbeatRequest
	regionReqs []*pb.RegionHeartbeatRequest
	removeReqs []*pb.RemoveRegionRequest
	storeResp  *pb.StoreHeartbeatResponse
	storeErr   error
	regionErr  error
	removeErr  error
	closed     bool
}

func (f *fakePDClient) StoreHeartbeat(_ context.Context, req *pb.StoreHeartbeatRequest) (*pb.StoreHeartbeatResponse, error) {
	f.storeReqs = append(f.storeReqs, req)
	if f.storeErr != nil {
		return nil, f.storeErr
	}
	if f.storeResp != nil {
		return f.storeResp, nil
	}
	return &pb.StoreHeartbeatResponse{Accepted: true}, nil
}

func (f *fakePDClient) RegionHeartbeat(_ context.Context, req *pb.RegionHeartbeatRequest) (*pb.RegionHeartbeatResponse, error) {
	f.regionReqs = append(f.regionReqs, req)
	if f.regionErr != nil {
		return nil, f.regionErr
	}
	return &pb.RegionHeartbeatResponse{Accepted: true}, nil
}

func (f *fakePDClient) RemoveRegion(_ context.Context, req *pb.RemoveRegionRequest) (*pb.RemoveRegionResponse, error) {
	f.removeReqs = append(f.removeReqs, req)
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	return &pb.RemoveRegionResponse{Removed: true}, nil
}

func (f *fakePDClient) GetRegionByKey(context.Context, *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error) {
	return &pb.GetRegionByKeyResponse{}, nil
}

func (f *fakePDClient) AllocID(context.Context, *pb.AllocIDRequest) (*pb.AllocIDResponse, error) {
	return &pb.AllocIDResponse{}, nil
}

func (f *fakePDClient) Tso(context.Context, *pb.TsoRequest) (*pb.TsoResponse, error) {
	return &pb.TsoResponse{}, nil
}

func (f *fakePDClient) Close() error {
	f.closed = true
	return nil
}

func TestRegionSinkMirrorsAndForwards(t *testing.T) {
	mirror := scheduler.NewCoordinator()
	pd := &fakePDClient{
		storeResp: &pb.StoreHeartbeatResponse{
			Accepted: true,
			Operations: []*pb.SchedulerOperation{
				{
					Type:         pb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
					RegionId:     10,
					SourcePeerId: 101,
					TargetPeerId: 201,
				},
			},
		},
	}
	sink := NewRegionSink(RegionSinkConfig{
		PD:     pd,
		Mirror: mirror,
	})

	meta := manifest.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []manifest.PeerMeta{{StoreID: 1, PeerID: 101}},
	}
	sink.SubmitRegionHeartbeat(meta)
	sink.SubmitStoreHeartbeat(scheduler.StoreStats{
		StoreID:   1,
		RegionNum: 3,
		LeaderNum: 1,
		Capacity:  1000,
		Available: 800,
	})

	require.Len(t, pd.regionReqs, 1)
	require.Equal(t, uint64(10), pd.regionReqs[0].GetRegion().GetId())
	require.Len(t, pd.storeReqs, 1)
	require.Equal(t, uint64(1), pd.storeReqs[0].GetStoreId())

	regions := sink.RegionSnapshot()
	require.Len(t, regions, 1)
	require.Equal(t, uint64(10), regions[0].Meta.ID)
	stores := sink.StoreSnapshot()
	require.Len(t, stores, 1)
	require.Equal(t, uint64(1), stores[0].StoreID)

	ops := sink.Plan(scheduler.Snapshot{})
	require.Len(t, ops, 1)
	require.Equal(t, scheduler.OperationLeaderTransfer, ops[0].Type)
	require.Equal(t, uint64(10), ops[0].Region)
	require.Equal(t, uint64(101), ops[0].Source)
	require.Equal(t, uint64(201), ops[0].Target)
	require.Nil(t, sink.Plan(scheduler.Snapshot{}), "Plan should drain pending ops")
}

func TestRegionSinkErrorCallbackAndClose(t *testing.T) {
	storeErr := errors.New("store heartbeat failed")
	regionErr := errors.New("region heartbeat failed")
	pd := &fakePDClient{
		storeErr:  storeErr,
		regionErr: regionErr,
	}
	var got []string
	sink := NewRegionSink(RegionSinkConfig{
		PD: pd,
		OnError: func(op string, err error) {
			got = append(got, op+": "+err.Error())
		},
	})

	sink.SubmitStoreHeartbeat(scheduler.StoreStats{StoreID: 7})
	sink.SubmitRegionHeartbeat(manifest.RegionMeta{ID: 9})
	require.Len(t, got, 2)
	require.Contains(t, got[0], "StoreHeartbeat")
	require.Contains(t, got[1], "RegionHeartbeat")
	require.NoError(t, sink.Close())
	require.True(t, pd.closed)
}

func TestRegionSinkNoopOnZeroIDs(t *testing.T) {
	pd := &fakePDClient{}
	sink := NewRegionSink(RegionSinkConfig{PD: pd})
	sink.SubmitStoreHeartbeat(scheduler.StoreStats{StoreID: 0})
	sink.SubmitRegionHeartbeat(manifest.RegionMeta{ID: 0})
	sink.RemoveRegion(0)
	require.Empty(t, pd.storeReqs)
	require.Empty(t, pd.regionReqs)
	require.Empty(t, pd.removeReqs)
}

func TestRegionSinkRemoveRegionForwardsAndReportsErrors(t *testing.T) {
	removeErr := errors.New("remove region failed")
	pd := &fakePDClient{removeErr: removeErr}
	mirror := scheduler.NewCoordinator()
	var got []string
	sink := NewRegionSink(RegionSinkConfig{
		PD:     pd,
		Mirror: mirror,
		OnError: func(op string, err error) {
			got = append(got, op+": "+err.Error())
		},
	})

	meta := manifest.RegionMeta{
		ID:       100,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
	}
	sink.SubmitRegionHeartbeat(meta)
	require.NotEmpty(t, sink.RegionSnapshot())

	sink.RemoveRegion(100)
	require.Len(t, pd.removeReqs, 1)
	require.Equal(t, uint64(100), pd.removeReqs[0].GetRegionId())
	require.Empty(t, sink.RegionSnapshot(), "mirror should be updated even when PD RPC fails")
	require.Len(t, got, 1)
	require.Contains(t, got[0], "RemoveRegion")
}

func TestFromPBOperationValidation(t *testing.T) {
	_, ok := fromPBOperation(nil)
	require.False(t, ok)
	_, ok = fromPBOperation(&pb.SchedulerOperation{
		Type: pb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_NONE,
	})
	require.False(t, ok)

	op, ok := fromPBOperation(&pb.SchedulerOperation{
		Type:         pb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
		RegionId:     1,
		SourcePeerId: 10,
		TargetPeerId: 20,
	})
	require.True(t, ok)
	require.Equal(t, scheduler.OperationLeaderTransfer, op.Type)
}
