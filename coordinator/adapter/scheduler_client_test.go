package adapter

import (
	"context"
	"errors"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"testing"

	"github.com/stretchr/testify/require"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

type fakePDClient struct {
	storeReqs    []*coordpb.StoreHeartbeatRequest
	livenessReqs []*coordpb.RegionLivenessRequest
	rootEventReq []*coordpb.PublishRootEventRequest
	removeReqs   []*coordpb.RemoveRegionRequest
	storeResp    *coordpb.StoreHeartbeatResponse
	storeErr     error
	livenessErr  error
	rootErr      error
	removeErr    error
	closed       bool
}

func (f *fakePDClient) StoreHeartbeat(_ context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	f.storeReqs = append(f.storeReqs, req)
	if f.storeErr != nil {
		return nil, f.storeErr
	}
	if f.storeResp != nil {
		return f.storeResp, nil
	}
	return &coordpb.StoreHeartbeatResponse{Accepted: true}, nil
}

func (f *fakePDClient) RegionLiveness(_ context.Context, req *coordpb.RegionLivenessRequest) (*coordpb.RegionLivenessResponse, error) {
	f.livenessReqs = append(f.livenessReqs, req)
	if f.livenessErr != nil {
		return nil, f.livenessErr
	}
	return &coordpb.RegionLivenessResponse{Accepted: true}, nil
}

func (f *fakePDClient) PublishRootEvent(_ context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	f.rootEventReq = append(f.rootEventReq, req)
	if f.rootErr != nil {
		return nil, f.rootErr
	}
	return &coordpb.PublishRootEventResponse{Accepted: true}, nil
}

func (f *fakePDClient) ListTransitions(context.Context, *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error) {
	return &coordpb.ListTransitionsResponse{}, nil
}

func (f *fakePDClient) AssessRootEvent(context.Context, *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error) {
	return &coordpb.AssessRootEventResponse{}, nil
}

func (f *fakePDClient) RemoveRegion(_ context.Context, req *coordpb.RemoveRegionRequest) (*coordpb.RemoveRegionResponse, error) {
	f.removeReqs = append(f.removeReqs, req)
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	return &coordpb.RemoveRegionResponse{Removed: true}, nil
}

func (f *fakePDClient) GetRegionByKey(context.Context, *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	return &coordpb.GetRegionByKeyResponse{}, nil
}

func (f *fakePDClient) AllocID(context.Context, *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	return &coordpb.AllocIDResponse{}, nil
}

func (f *fakePDClient) Tso(context.Context, *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	return &coordpb.TsoResponse{}, nil
}

func (f *fakePDClient) Close() error {
	f.closed = true
	return nil
}

func TestSchedulerClientPublishRootEvent(t *testing.T) {
	pd := &fakePDClient{}
	sink := NewSchedulerClient(SchedulerClientConfig{Coordinator: pd})

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
		storeResp: &coordpb.StoreHeartbeatResponse{
			Accepted: true,
			Operations: []*coordpb.SchedulerOperation{
				{
					Type:         coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
					RegionId:     10,
					SourcePeerId: 101,
					TargetPeerId: 201,
				},
			},
		},
	}
	sink := NewSchedulerClient(SchedulerClientConfig{
		Coordinator: pd,
	})

	sink.ReportRegionHeartbeat(context.Background(), 10)
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
		Coordinator: pd,
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
	sink := NewSchedulerClient(SchedulerClientConfig{Coordinator: pd})
	sink.StoreHeartbeat(context.Background(), storepkg.StoreStats{StoreID: 0})
	sink.ReportRegionHeartbeat(context.Background(), 0)
	require.NoError(t, sink.PublishRootEvent(context.Background(), rootevent.Event{}))
	require.Empty(t, pd.storeReqs)
	require.Empty(t, pd.livenessReqs)
	require.Empty(t, pd.rootEventReq)
}

func TestSchedulerClientRejectsConflictingRootEpochsInOneEvent(t *testing.T) {
	pd := &fakePDClient{}
	var got []string
	sink := NewSchedulerClient(SchedulerClientConfig{
		Coordinator: pd,
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
	sink := NewSchedulerClient(SchedulerClientConfig{Coordinator: pd})

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
	_, ok = fromPBOperation(&coordpb.SchedulerOperation{
		Type: coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_NONE,
	})
	require.False(t, ok)

	op, ok := fromPBOperation(&coordpb.SchedulerOperation{
		Type:         coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
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
