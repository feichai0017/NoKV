package storecontrol

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

type fakeCoordinatorClient struct {
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

func (f *fakeCoordinatorClient) StoreHeartbeat(_ context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	f.storeReqs = append(f.storeReqs, req)
	if f.storeErr != nil {
		return nil, f.storeErr
	}
	if f.storeResp != nil {
		return f.storeResp, nil
	}
	return &coordpb.StoreHeartbeatResponse{Accepted: true}, nil
}

func (f *fakeCoordinatorClient) RegionLiveness(_ context.Context, req *coordpb.RegionLivenessRequest) (*coordpb.RegionLivenessResponse, error) {
	f.livenessReqs = append(f.livenessReqs, req)
	if f.livenessErr != nil {
		return nil, f.livenessErr
	}
	return &coordpb.RegionLivenessResponse{Accepted: true}, nil
}

func (f *fakeCoordinatorClient) PublishRootEvent(_ context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	f.rootEventReq = append(f.rootEventReq, req)
	if f.rootErr != nil {
		return nil, f.rootErr
	}
	return &coordpb.PublishRootEventResponse{Accepted: true}, nil
}

func (f *fakeCoordinatorClient) ListTransitions(context.Context, *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error) {
	return &coordpb.ListTransitionsResponse{}, nil
}

func (f *fakeCoordinatorClient) AssessRootEvent(context.Context, *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error) {
	return &coordpb.AssessRootEventResponse{}, nil
}

func (f *fakeCoordinatorClient) RemoveRegion(_ context.Context, req *coordpb.RemoveRegionRequest) (*coordpb.RemoveRegionResponse, error) {
	f.removeReqs = append(f.removeReqs, req)
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	return &coordpb.RemoveRegionResponse{Removed: true}, nil
}

func (f *fakeCoordinatorClient) GetRegionByKey(context.Context, *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	return &coordpb.GetRegionByKeyResponse{}, nil
}

func (f *fakeCoordinatorClient) GetStore(context.Context, *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	return &coordpb.GetStoreResponse{}, nil
}

func (f *fakeCoordinatorClient) ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error) {
	return &coordpb.ListStoresResponse{}, nil
}

func (f *fakeCoordinatorClient) GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	return &coordpb.GetMountResponse{}, nil
}

func (f *fakeCoordinatorClient) ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	return &coordpb.ListMountsResponse{}, nil
}

func (f *fakeCoordinatorClient) ListSubtreeAuthorities(context.Context, *coordpb.ListSubtreeAuthoritiesRequest) (*coordpb.ListSubtreeAuthoritiesResponse, error) {
	return &coordpb.ListSubtreeAuthoritiesResponse{}, nil
}

func (f *fakeCoordinatorClient) GetQuotaFence(context.Context, *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error) {
	return &coordpb.GetQuotaFenceResponse{}, nil
}

func (f *fakeCoordinatorClient) ListQuotaFences(context.Context, *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error) {
	return &coordpb.ListQuotaFencesResponse{}, nil
}

func (f *fakeCoordinatorClient) ListPerasAuthorityGrants(context.Context, *coordpb.ListPerasAuthorityGrantsRequest) (*coordpb.ListPerasAuthorityGrantsResponse, error) {
	return &coordpb.ListPerasAuthorityGrantsResponse{}, nil
}

func (f *fakeCoordinatorClient) ListPerasAuthoritySeals(context.Context, *coordpb.ListPerasAuthoritySealsRequest) (*coordpb.ListPerasAuthoritySealsResponse, error) {
	return &coordpb.ListPerasAuthoritySealsResponse{}, nil
}

func (f *fakeCoordinatorClient) ApplyPerasAuthority(context.Context, *coordpb.ApplyPerasAuthorityRequest) (*coordpb.ApplyPerasAuthorityResponse, error) {
	return &coordpb.ApplyPerasAuthorityResponse{}, nil
}

func (f *fakeCoordinatorClient) WatchRootEvents(context.Context, *coordpb.WatchRootEventsRequest, ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
	return nil, nil
}

func (f *fakeCoordinatorClient) AllocID(context.Context, *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	return &coordpb.AllocIDResponse{}, nil
}

func (f *fakeCoordinatorClient) Tso(context.Context, *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	return &coordpb.TsoResponse{}, nil
}

func (f *fakeCoordinatorClient) Close() error {
	f.closed = true
	return nil
}

func TestClientPublishRootEvent(t *testing.T) {
	coord := &fakeCoordinatorClient{}
	sink := NewClient(Config{Coordinator: coord})

	event := rootevent.PeerAdded(10, 2, 201, testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 2,
	}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}}))
	require.NoError(t, sink.PublishRootEvent(context.Background(), event))

	require.Len(t, coord.rootEventReq, 1)
	require.Equal(t, uint64(1), coord.rootEventReq[0].GetExpectedClusterEpoch())
	got := metawire.RootEventFromProto(coord.rootEventReq[0].GetEvent())
	require.Equal(t, rootevent.KindPeerAdded, got.Kind)
	require.NotNil(t, got.PeerChange)
	require.Equal(t, uint64(10), got.PeerChange.RegionID)
	require.Equal(t, uint64(0), got.PeerChange.Region.RootEpoch)
	require.False(t, sink.Status().Degraded)
}

func TestClientInitialStatusIsHealthy(t *testing.T) {
	sink := NewClient(Config{})

	status := sink.Status()
	require.Equal(t, ModeHealthy, status.Mode)
	require.False(t, status.Degraded)
	require.Empty(t, status.LastError)
}

func TestClientForwardsAndPlans(t *testing.T) {
	coord := &fakeCoordinatorClient{
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
	sink := NewClient(Config{
		Coordinator: coord,
	})

	sink.ReportRegionHeartbeat(context.Background(), 10)
	ops := sink.StoreHeartbeat(context.Background(), StoreStats{
		StoreID:           1,
		RegionNum:         3,
		LeaderNum:         1,
		Capacity:          1000,
		Available:         800,
		DroppedOperations: 7,
		LeaderRegionIDs:   []uint64{10, 12},
		RegionStats: []RegionStats{{
			RegionID:            10,
			ReadQPS:             11,
			WriteQPS:            12,
			WriteBytesPerSecond: 13,
			ApproxRegionBytes:   14,
			AtomicMutateQPS:     15,
			LeaderStoreID:       1,
			PendingAdmin:        true,
		}},
	})

	require.Len(t, coord.livenessReqs, 1)
	require.Equal(t, uint64(10), coord.livenessReqs[0].GetRegionId())
	require.Len(t, coord.storeReqs, 1)
	require.Equal(t, uint64(1), coord.storeReqs[0].GetStoreId())
	require.Equal(t, uint64(7), coord.storeReqs[0].GetDroppedOperations())
	require.Equal(t, []uint64{10, 12}, coord.storeReqs[0].GetLeaderRegionIds())
	require.Len(t, coord.storeReqs[0].GetRegionStats(), 1)
	require.Equal(t, uint64(10), coord.storeReqs[0].GetRegionStats()[0].GetRegionId())
	require.Equal(t, uint64(12), coord.storeReqs[0].GetRegionStats()[0].GetWriteQps())
	require.True(t, coord.storeReqs[0].GetRegionStats()[0].GetPendingAdmin())

	require.Len(t, ops, 1)
	require.Equal(t, OperationLeaderTransfer, ops[0].Type)
	require.Equal(t, uint64(10), ops[0].Region)
	require.Equal(t, uint64(101), ops[0].Source)
	require.Equal(t, uint64(201), ops[0].Target)
	require.False(t, sink.Status().Degraded)
}

func TestClientErrorCallbackAndClose(t *testing.T) {
	storeErr := errors.New("store heartbeat failed")
	rootErr := errors.New("publish root event failed")
	coord := &fakeCoordinatorClient{
		storeErr: storeErr,
		rootErr:  rootErr,
	}
	var got []string
	sink := NewClient(Config{
		Coordinator: coord,
		OnError: func(op string, err error) {
			got = append(got, op+": "+err.Error())
		},
	})

	sink.StoreHeartbeat(context.Background(), StoreStats{StoreID: 7})
	require.Error(t, sink.PublishRootEvent(context.Background(), rootevent.RegionDescriptorPublished(testDescriptor(9, nil, nil, metaregion.Epoch{}, nil))))
	require.Len(t, got, 2)
	require.Contains(t, got[0], "StoreHeartbeat")
	require.Contains(t, got[1], "PublishRootEvent")
	status := sink.Status()
	require.True(t, status.Degraded)
	require.Equal(t, ModeUnavailable, status.Mode)
	require.Contains(t, status.LastError, "PublishRootEvent")
	require.False(t, status.LastErrorAt.IsZero())
	require.NoError(t, sink.Close())
	require.True(t, coord.closed)
}

func TestClientNoopOnZeroIDs(t *testing.T) {
	coord := &fakeCoordinatorClient{}
	sink := NewClient(Config{Coordinator: coord})
	sink.StoreHeartbeat(context.Background(), StoreStats{StoreID: 0})
	sink.ReportRegionHeartbeat(context.Background(), 0)
	require.NoError(t, sink.PublishRootEvent(context.Background(), rootevent.Event{}))
	require.Empty(t, coord.storeReqs)
	require.Empty(t, coord.livenessReqs)
	require.Empty(t, coord.rootEventReq)
}

func TestClientRejectsConflictingRootEpochsInOneEvent(t *testing.T) {
	coord := &fakeCoordinatorClient{}
	var got []string
	sink := NewClient(Config{
		Coordinator: coord,
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
	require.Empty(t, coord.rootEventReq)
	require.Len(t, got, 1)
	require.Contains(t, got[0], "conflicting root epochs")
	require.True(t, sink.Status().Degraded)
}

func TestClientStatusRecoversAfterSuccess(t *testing.T) {
	coord := &fakeCoordinatorClient{storeErr: errors.New("heartbeat failed")}
	sink := NewClient(Config{Coordinator: coord})

	sink.StoreHeartbeat(context.Background(), StoreStats{StoreID: 7})
	require.True(t, sink.Status().Degraded)

	coord.storeErr = nil
	sink.StoreHeartbeat(context.Background(), StoreStats{StoreID: 7})
	status := sink.Status()
	require.False(t, status.Degraded)
	require.Equal(t, ModeHealthy, status.Mode)
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
	require.Equal(t, OperationLeaderTransfer, op.Type)

	child := testDescriptor(2, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	op, ok = fromPBOperation(&coordpb.SchedulerOperation{
		Type:       coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_SPLIT_REGION,
		RegionId:   1,
		SplitKey:   []byte("m"),
		SplitChild: metawire.DescriptorToProto(child),
	})
	require.True(t, ok)
	require.Equal(t, OperationSplitRegion, op.Type)
	require.Equal(t, []byte("m"), op.SplitKey)
	require.Equal(t, uint64(2), op.SplitChild.RegionID)

	op, ok = fromPBOperation(&coordpb.SchedulerOperation{
		Type:           coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_MERGE_REGION,
		RegionId:       1,
		SourceRegionId: 2,
	})
	require.True(t, ok)
	require.Equal(t, OperationMergeRegion, op.Type)
	require.Equal(t, uint64(2), op.SourceRegion)
}

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch, peers []metaregion.Peer) topology.Descriptor {
	desc := topology.Descriptor{
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
