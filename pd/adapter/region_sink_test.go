package adapter

import (
	"context"
	"errors"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"testing"

	"github.com/stretchr/testify/require"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

type fakePDClient struct {
	storeReqs  []*pdpb.StoreHeartbeatRequest
	regionReqs []*pdpb.RegionHeartbeatRequest
	removeReqs []*pdpb.RemoveRegionRequest
	storeResp  *pdpb.StoreHeartbeatResponse
	storeErr   error
	regionErr  error
	removeErr  error
	closed     bool
}

func (f *fakePDClient) StoreHeartbeat(_ context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	f.storeReqs = append(f.storeReqs, req)
	if f.storeErr != nil {
		return nil, f.storeErr
	}
	if f.storeResp != nil {
		return f.storeResp, nil
	}
	return &pdpb.StoreHeartbeatResponse{Accepted: true}, nil
}

func (f *fakePDClient) RegionHeartbeat(_ context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error) {
	f.regionReqs = append(f.regionReqs, req)
	if f.regionErr != nil {
		return nil, f.regionErr
	}
	return &pdpb.RegionHeartbeatResponse{Accepted: true}, nil
}

func (f *fakePDClient) RemoveRegion(_ context.Context, req *pdpb.RemoveRegionRequest) (*pdpb.RemoveRegionResponse, error) {
	f.removeReqs = append(f.removeReqs, req)
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	return &pdpb.RemoveRegionResponse{Removed: true}, nil
}

func (f *fakePDClient) GetRegionByKey(context.Context, *pdpb.GetRegionByKeyRequest) (*pdpb.GetRegionByKeyResponse, error) {
	return &pdpb.GetRegionByKeyResponse{}, nil
}

func (f *fakePDClient) AllocID(context.Context, *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	return &pdpb.AllocIDResponse{}, nil
}

func (f *fakePDClient) Tso(context.Context, *pdpb.TsoRequest) (*pdpb.TsoResponse, error) {
	return &pdpb.TsoResponse{}, nil
}

func (f *fakePDClient) Close() error {
	f.closed = true
	return nil
}

func TestSchedulerClientForwardsAndPlans(t *testing.T) {
	pd := &fakePDClient{
		storeResp: &pdpb.StoreHeartbeatResponse{
			Accepted: true,
			Operations: []*pdpb.SchedulerOperation{
				{
					Type:         pdpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
					RegionId:     10,
					SourcePeerId: 101,
					TargetPeerId: 201,
				},
			},
		},
	}
	sink := NewSchedulerClient(SchedulerClientConfig{
		PD: pd,
	})

	meta := localmeta.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []metaregion.Peer{{StoreID: 1, PeerID: 101}},
	}
	sink.PublishRegionDescriptor(context.Background(), metacodec.DescriptorFromLocalRegionMeta(meta, 0))
	ops := sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{
		StoreID:   1,
		RegionNum: 3,
		LeaderNum: 1,
		Capacity:  1000,
		Available: 800,
	})

	require.Len(t, pd.regionReqs, 1)
	require.Equal(t, uint64(10), pd.regionReqs[0].GetRegionDescriptor().GetRegionId())
	require.Len(t, pd.storeReqs, 1)
	require.Equal(t, uint64(1), pd.storeReqs[0].GetStoreId())

	require.Len(t, ops, 1)
	require.Equal(t, storepkg.OperationLeaderTransfer, ops[0].Type)
	require.Equal(t, uint64(10), ops[0].Region)
	require.Equal(t, uint64(101), ops[0].Source)
	require.Equal(t, uint64(201), ops[0].Target)
	require.False(t, sink.Status().Degraded)
}

func TestSchedulerClientErrorCallbackAndClose(t *testing.T) {
	storeErr := errors.New("store heartbeat failed")
	regionErr := errors.New("region heartbeat failed")
	pd := &fakePDClient{
		storeErr:  storeErr,
		regionErr: regionErr,
	}
	var got []string
	sink := NewSchedulerClient(SchedulerClientConfig{
		PD: pd,
		OnError: func(op string, err error) {
			got = append(got, op+": "+err.Error())
		},
	})

	sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{StoreID: 7})
	sink.PublishRegionDescriptor(context.Background(), metacodec.DescriptorFromLocalRegionMeta(localmeta.RegionMeta{ID: 9}, 0))
	require.Len(t, got, 2)
	require.Contains(t, got[0], "StoreHeartbeat")
	require.Contains(t, got[1], "RegionHeartbeat")
	status := sink.Status()
	require.True(t, status.Degraded)
	require.Equal(t, storepkg.SchedulerModeUnavailable, status.Mode)
	require.Contains(t, status.LastError, "RegionHeartbeat")
	require.False(t, status.LastErrorAt.IsZero())
	require.NoError(t, sink.Close())
	require.True(t, pd.closed)
}

func TestSchedulerClientNoopOnZeroIDs(t *testing.T) {
	pd := &fakePDClient{}
	sink := NewSchedulerClient(SchedulerClientConfig{PD: pd})
	sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{StoreID: 0})
	sink.PublishRegionDescriptor(context.Background(), descriptor.Descriptor{})
	sink.RemoveRegion(context.Background(), 0)
	require.Empty(t, pd.storeReqs)
	require.Empty(t, pd.regionReqs)
	require.Empty(t, pd.removeReqs)
}

func TestSchedulerClientRemoveRegionForwardsAndReportsErrors(t *testing.T) {
	removeErr := errors.New("remove region failed")
	pd := &fakePDClient{removeErr: removeErr}
	var got []string
	sink := NewSchedulerClient(SchedulerClientConfig{
		PD: pd,
		OnError: func(op string, err error) {
			got = append(got, op+": "+err.Error())
		},
	})

	meta := localmeta.RegionMeta{
		ID:       100,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
	}
	sink.PublishRegionDescriptor(context.Background(), metacodec.DescriptorFromLocalRegionMeta(meta, 0))

	sink.RemoveRegion(context.Background(), 100)
	require.Len(t, pd.removeReqs, 1)
	require.Equal(t, uint64(100), pd.removeReqs[0].GetRegionId())
	require.Len(t, got, 1)
	require.Contains(t, got[0], "RemoveRegion")
	require.True(t, sink.Status().Degraded)
	require.Equal(t, storepkg.SchedulerModeUnavailable, sink.Status().Mode)
}

func TestSchedulerClientStatusRecoversAfterSuccess(t *testing.T) {
	pd := &fakePDClient{storeErr: errors.New("heartbeat failed")}
	sink := NewSchedulerClient(SchedulerClientConfig{PD: pd})

	sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{StoreID: 7})
	require.True(t, sink.Status().Degraded)

	pd.storeErr = nil
	sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{StoreID: 7})
	status := sink.Status()
	require.False(t, status.Degraded)
	require.Equal(t, storepkg.SchedulerModeHealthy, status.Mode)
	require.Contains(t, status.LastError, "StoreHeartbeat")
}

func TestFromPBOperationValidation(t *testing.T) {
	_, ok := fromPBOperation(nil)
	require.False(t, ok)
	_, ok = fromPBOperation(&pdpb.SchedulerOperation{
		Type: pdpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_NONE,
	})
	require.False(t, ok)

	op, ok := fromPBOperation(&pdpb.SchedulerOperation{
		Type:         pdpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
		RegionId:     1,
		SourcePeerId: 10,
		TargetPeerId: 20,
	})
	require.True(t, ok)
	require.Equal(t, storepkg.OperationLeaderTransfer, op.Type)
}
