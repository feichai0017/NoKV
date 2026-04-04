package adapter

import (
	"context"
	"errors"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"testing"

	"github.com/stretchr/testify/require"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

type fakePDClient struct {
	storeReqs    []*pdpb.StoreHeartbeatRequest
	regionReqs   []*pdpb.RegionHeartbeatRequest
	livenessReqs []*pdpb.RegionLivenessRequest
	rootEventReq []*pdpb.PublishRootEventRequest
	removeReqs   []*pdpb.RemoveRegionRequest
	storeResp    *pdpb.StoreHeartbeatResponse
	storeErr     error
	regionErr    error
	rootErr      error
	removeErr    error
	closed       bool
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

func (f *fakePDClient) RegionLiveness(_ context.Context, req *pdpb.RegionLivenessRequest) (*pdpb.RegionLivenessResponse, error) {
	f.livenessReqs = append(f.livenessReqs, req)
	if f.regionErr != nil {
		return nil, f.regionErr
	}
	return &pdpb.RegionLivenessResponse{Accepted: true}, nil
}

func (f *fakePDClient) PublishRootEvent(_ context.Context, req *pdpb.PublishRootEventRequest) (*pdpb.PublishRootEventResponse, error) {
	f.rootEventReq = append(f.rootEventReq, req)
	if f.rootErr != nil {
		return nil, f.rootErr
	}
	return &pdpb.PublishRootEventResponse{Accepted: true}, nil
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

func TestSchedulerClientPublishRootEvent(t *testing.T) {
	pd := &fakePDClient{}
	sink := NewSchedulerClient(SchedulerClientConfig{PD: pd})

	event := rootevent.PeerAdded(10, 2, 201, testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 2,
	}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}}))
	require.NoError(t, sink.PublishRootEvent(context.Background(), event))

	require.Len(t, pd.rootEventReq, 1)
	require.Equal(t, uint64(1), pd.rootEventReq[0].GetExpectedClusterEpoch())
	got := metacodec.RootEventFromProto(pd.rootEventReq[0].GetEvent())
	require.Equal(t, rootevent.KindPeerAdded, got.Kind)
	require.NotNil(t, got.PeerChange)
	require.Equal(t, uint64(10), got.PeerChange.RegionID)
	require.Equal(t, uint64(0), got.PeerChange.Region.RootEpoch)
	require.False(t, sink.Status().Degraded)
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

	sink.ReportRegionHeartbeat(context.Background(), testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	}, []metaregion.Peer{{StoreID: 1, PeerID: 101}}))
	ops := sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{
		StoreID:   1,
		RegionNum: 3,
		LeaderNum: 1,
		Capacity:  1000,
		Available: 800,
	})

	require.Len(t, pd.livenessReqs, 1)
	require.Equal(t, uint64(10), pd.livenessReqs[0].GetRegionId())
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
	rootErr := errors.New("publish root event failed")
	pd := &fakePDClient{
		storeErr: storeErr,
		rootErr:  rootErr,
	}
	var got []string
	sink := NewSchedulerClient(SchedulerClientConfig{
		PD: pd,
		OnError: func(op string, err error) {
			got = append(got, op+": "+err.Error())
		},
	})

	sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{StoreID: 7})
	require.Error(t, sink.PublishRootEvent(context.Background(), rootevent.RegionDescriptorPublished(testDescriptor(9, nil, nil, metaregion.Epoch{}, nil))))
	require.Len(t, got, 2)
	require.Contains(t, got[0], "StoreHeartbeat")
	require.Contains(t, got[1], "PublishRootEvent")
	status := sink.Status()
	require.True(t, status.Degraded)
	require.Equal(t, storepkg.SchedulerModeUnavailable, status.Mode)
	require.Contains(t, status.LastError, "PublishRootEvent")
	require.False(t, status.LastErrorAt.IsZero())
	require.NoError(t, sink.Close())
	require.True(t, pd.closed)
}

func TestSchedulerClientNoopOnZeroIDs(t *testing.T) {
	pd := &fakePDClient{}
	sink := NewSchedulerClient(SchedulerClientConfig{PD: pd})
	sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{StoreID: 0})
	sink.ReportRegionHeartbeat(context.Background(), descriptor.Descriptor{})
	require.NoError(t, sink.PublishRootEvent(context.Background(), rootevent.Event{}))
	require.Empty(t, pd.storeReqs)
	require.Empty(t, pd.regionReqs)
	require.Empty(t, pd.livenessReqs)
	require.Empty(t, pd.rootEventReq)
}

func TestSchedulerClientRejectsConflictingRootEpochsInOneEvent(t *testing.T) {
	pd := &fakePDClient{}
	var got []string
	sink := NewSchedulerClient(SchedulerClientConfig{
		PD: pd,
		OnError: func(op string, err error) {
			got = append(got, op+": "+err.Error())
		},
	})

	left := testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	left.RootEpoch = 7
	right.RootEpoch = 8
	left.EnsureHash()
	right.EnsureHash()

	require.Error(t, sink.PublishRootEvent(context.Background(), rootevent.RegionSplitCommitted(41, []byte("m"), left, right)))
	require.Empty(t, pd.rootEventReq)
	require.Len(t, got, 1)
	require.Contains(t, got[0], "conflicting root epochs")
	require.True(t, sink.Status().Degraded)
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

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch, peers []metaregion.Peer) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     epoch,
		Peers:     append([]metaregion.Peer(nil), peers...),
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
