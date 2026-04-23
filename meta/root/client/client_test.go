package client

import (
	"context"
	"testing"
	"time"

	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryableRemoteErrorConnectionClosing(t *testing.T) {
	err := status.Error(codes.Canceled, errClientConnectionClosing)
	require.True(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorWrappedConnectionClosing(t *testing.T) {
	inner := status.Error(codes.Canceled, errClientConnectionClosing)
	err := status.Error(codes.Internal, inner.Error())
	require.True(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorMetadataRootNotLeaderIsWriteOnly(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, errMetadataRootNotLeader+" (leader_id=2)")
	require.False(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorLeavesGenericInternalFatal(t *testing.T) {
	err := status.Error(codes.Internal, "boom")
	require.False(t, retryableRemoteError(err, false))
	require.False(t, retryableRemoteError(err, true))
}

type fakeMetadataRootClient struct {
	statusFunc func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error)
}

func (f *fakeMetadataRootClient) Snapshot(context.Context, *metapb.MetadataRootSnapshotRequest, ...grpc.CallOption) (*metapb.MetadataRootSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "snapshot")
}

func (f *fakeMetadataRootClient) Append(context.Context, *metapb.MetadataRootAppendRequest, ...grpc.CallOption) (*metapb.MetadataRootAppendResponse, error) {
	return nil, status.Error(codes.Unimplemented, "append")
}

func (f *fakeMetadataRootClient) FenceAllocator(context.Context, *metapb.MetadataRootFenceAllocatorRequest, ...grpc.CallOption) (*metapb.MetadataRootFenceAllocatorResponse, error) {
	return nil, status.Error(codes.Unimplemented, "fence")
}

func (f *fakeMetadataRootClient) Status(ctx context.Context, req *metapb.MetadataRootStatusRequest, opts ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error) {
	if f.statusFunc != nil {
		return f.statusFunc(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "status")
}

func (f *fakeMetadataRootClient) ApplyTenure(context.Context, *metapb.MetadataRootApplyTenureRequest, ...grpc.CallOption) (*metapb.MetadataRootApplyTenureResponse, error) {
	return nil, status.Error(codes.Unimplemented, "lease")
}

func (f *fakeMetadataRootClient) ApplyHandover(context.Context, *metapb.MetadataRootApplyHandoverRequest, ...grpc.CallOption) (*metapb.MetadataRootApplyHandoverResponse, error) {
	return nil, status.Error(codes.Unimplemented, "handover")
}

func (f *fakeMetadataRootClient) ObserveCommitted(context.Context, *metapb.MetadataRootObserveCommittedRequest, ...grpc.CallOption) (*metapb.MetadataRootObserveCommittedResponse, error) {
	return nil, status.Error(codes.Unimplemented, "observe committed")
}

func (f *fakeMetadataRootClient) ObserveTail(context.Context, *metapb.MetadataRootObserveTailRequest, ...grpc.CallOption) (*metapb.MetadataRootObserveTailResponse, error) {
	return nil, status.Error(codes.Unimplemented, "observe tail")
}

func (f *fakeMetadataRootClient) WaitTail(context.Context, *metapb.MetadataRootWaitTailRequest, ...grpc.CallOption) (*metapb.MetadataRootWaitTailResponse, error) {
	return nil, status.Error(codes.Unimplemented, "wait tail")
}

func TestClientHelpersAndOrdering(t *testing.T) {
	c := &Client{
		endpoints: []clientEndpoint{{id: 1}, {id: 2}, {id: 3}},
		byID:      map[uint64]int{1: 0, 2: 1, 3: 2},
		preferred: 1,
	}

	ordered := c.orderedEndpoints()
	require.Equal(t, []uint64{2, 3, 1}, []uint64{ordered[0].id, ordered[1].id, ordered[2].id})

	endpoint, ok := c.endpointByID(3)
	require.True(t, ok)
	require.Equal(t, uint64(3), endpoint.id)

	c.markPreferred(1)
	require.Equal(t, uint64(1), c.orderedEndpoints()[0].id)

	ctx, cancel := c.context(context.TODO())
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(defaultCallTimeout), deadline, 150*time.Millisecond)

	c.callTimeout = 25 * time.Millisecond
	ctx, cancel = c.context(context.Background())
	defer cancel()
	deadline, ok = ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(25*time.Millisecond), deadline, 150*time.Millisecond)

	require.True(t, validTenureAct(1))
	require.False(t, validTenureAct(99))
	require.True(t, validHandoverAct(1))
	require.False(t, validHandoverAct(99))

	leaderID, ok := leaderHint(status.Error(codes.FailedPrecondition, errMetadataRootNotLeader+" (leader_id=23)"))
	require.True(t, ok)
	require.Equal(t, uint64(23), leaderID)
	_, ok = leaderHint(status.Error(codes.FailedPrecondition, "metadata root not leader"))
	require.False(t, ok)

	require.NoError(t, waitForReady(context.Background(), nil))
}

func TestInvokeRejectsNilCanceledAndEmptyClients(t *testing.T) {
	_, err := invokeRead[*metapb.MetadataRootStatusResponse](nil, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootStatusResponse, error) {
		return rpc.Status(ctx, &metapb.MetadataRootStatusRequest{})
	})
	require.ErrorIs(t, err, errNilClient)

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = invokeRead(&Client{byID: map[uint64]int{}}, parent, func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootStatusResponse, error) {
		return rpc.Status(ctx, &metapb.MetadataRootStatusRequest{})
	})
	require.ErrorIs(t, err, context.Canceled)

	_, err = invokeRead(&Client{byID: map[uint64]int{}}, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootStatusResponse, error) {
		return rpc.Status(ctx, &metapb.MetadataRootStatusRequest{})
	})
	require.ErrorIs(t, err, errNoEndpoints)
}

func TestInvokeWriteRetriesToLeaderHint(t *testing.T) {
	var followerCalls int
	var leaderCalls int
	c := &Client{
		endpoints: []clientEndpoint{
			{
				id: 1,
				rpc: &fakeMetadataRootClient{
					statusFunc: func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error) {
						followerCalls++
						return nil, status.Error(codes.FailedPrecondition, errMetadataRootNotLeader+" (leader_id=2)")
					},
				},
			},
			{
				id: 2,
				rpc: &fakeMetadataRootClient{
					statusFunc: func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error) {
						leaderCalls++
						return &metapb.MetadataRootStatusResponse{LeaderId: 2, IsLeader: true}, nil
					},
				},
			},
		},
		byID: map[uint64]int{1: 0, 2: 1},
	}

	resp, err := invokeWrite(c, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootStatusResponse, error) {
		return rpc.Status(ctx, &metapb.MetadataRootStatusRequest{})
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.GetLeaderId())
	require.Equal(t, 1, followerCalls)
	require.Equal(t, 1, leaderCalls)
	require.Equal(t, uint64(2), c.orderedEndpoints()[0].id)
}

func TestInvokeReadDoesNotRetryNotLeaderHint(t *testing.T) {
	var followerCalls int
	var leaderCalls int
	c := &Client{
		endpoints: []clientEndpoint{
			{
				id: 1,
				rpc: &fakeMetadataRootClient{
					statusFunc: func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error) {
						followerCalls++
						return nil, status.Error(codes.FailedPrecondition, errMetadataRootNotLeader+" (leader_id=2)")
					},
				},
			},
			{
				id: 2,
				rpc: &fakeMetadataRootClient{
					statusFunc: func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error) {
						leaderCalls++
						return &metapb.MetadataRootStatusResponse{LeaderId: 2, IsLeader: true}, nil
					},
				},
			},
		},
		byID: map[uint64]int{1: 0, 2: 1},
	}

	_, err := invokeRead(c, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootStatusResponse, error) {
		return rpc.Status(ctx, &metapb.MetadataRootStatusRequest{})
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), errMetadataRootNotLeader)
	require.Equal(t, 1, followerCalls)
	require.Zero(t, leaderCalls)
}

func TestPinLeaderPreferred(t *testing.T) {
	c := &Client{
		endpoints: []clientEndpoint{
			{
				id: 4,
				rpc: &fakeMetadataRootClient{
					statusFunc: func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error) {
						return &metapb.MetadataRootStatusResponse{}, nil
					},
				},
			},
			{
				id: 7,
				rpc: &fakeMetadataRootClient{
					statusFunc: func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error) {
						return &metapb.MetadataRootStatusResponse{LeaderId: 7}, nil
					},
				},
			},
		},
		byID: map[uint64]int{4: 0, 7: 1},
	}

	c.pinLeaderPreferred(context.Background())
	require.Equal(t, uint64(7), c.orderedEndpoints()[0].id)
}
