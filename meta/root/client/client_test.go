package client

import (
	"context"
	stderrors "errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootserver "github.com/feichai0017/NoKV/meta/root/server"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRetryableRemoteErrorConnectionClosing(t *testing.T) {
	err := clientConnClosingErrForTest()
	require.True(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorWrappedConnectionClosing(t *testing.T) {
	err := stderrors.Join(clientConnClosingErrForTest(), status.Error(codes.Internal, "transport closing"))
	require.True(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func clientConnClosingErrForTest() error {
	//nolint:staticcheck
	return grpc.ErrClientConnClosing
}

func TestDialClusterAcceptsLargeRootSnapshot(t *testing.T) {
	listener := bufconn.Listen(defaultMaxRootRPCMessageBytes)
	server := grpc.NewServer(rootserver.GRPCServerOptions()...)
	rootserver.Register(server, largeSnapshotBackend())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	ctx := context.Background()
	client, err := DialCluster(ctx, map[uint64]string{1: "passthrough:///bufnet"},
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	snapshot, err := client.Snapshot()
	require.NoError(t, err)
	require.Greater(t, len(snapshot.Subtrees), 4_000)
}

type largeRootBackend struct {
	snapshot rootstate.Snapshot
}

func largeSnapshotBackend() *largeRootBackend {
	subtrees := make(map[string]rootstate.SubtreeAuthority, 5_200)
	payload := strings.Repeat("x", 1024)
	for i := range 5_200 {
		rootInode := uint64(i + 1)
		id := fmt.Sprintf("bench/%d", rootInode)
		subtrees[id] = rootstate.SubtreeAuthority{
			SubtreeID:   id,
			Mount:       "bench",
			RootInode:   rootInode,
			AuthorityID: payload,
			Era:         1,
			State:       rootstate.SubtreeAuthorityActive,
		}
	}
	return &largeRootBackend{
		snapshot: rootstate.Snapshot{
			Subtrees: subtrees,
		},
	}
}

func (b *largeRootBackend) Snapshot() (rootstate.Snapshot, error) {
	return rootstate.CloneSnapshot(b.snapshot), nil
}

func (b *largeRootBackend) Append(context.Context, ...rootevent.Event) (rootstate.CommitInfo, error) {
	return rootstate.CommitInfo{}, nil
}

func (b *largeRootBackend) FenceAllocator(context.Context, rootstate.AllocatorKind, uint64) (uint64, error) {
	return 0, nil
}

func TestRetryableRemoteErrorMetadataRootNotLeaderIsWriteOnly(t *testing.T) {
	err := metadataRootNotLeaderErrorForTest(2)
	require.False(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorLeavesGenericInternalFatal(t *testing.T) {
	err := status.Error(codes.Internal, "boom")
	require.False(t, retryableRemoteError(err, false))
	require.False(t, retryableRemoteError(err, true))
}

type fakeMetadataRootClient struct {
	statusFunc                func(context.Context, *metapb.MetadataRootStatusRequest, ...grpc.CallOption) (*metapb.MetadataRootStatusResponse, error)
	applyCapsuleAuthorityFunc func(context.Context, *metapb.MetadataRootApplyCapsuleAuthorityRequest, ...grpc.CallOption) (*metapb.MetadataRootApplyCapsuleAuthorityResponse, error)
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

func (f *fakeMetadataRootClient) ApplyGrant(context.Context, *metapb.MetadataRootApplyGrantRequest, ...grpc.CallOption) (*metapb.MetadataRootApplyGrantResponse, error) {
	return nil, status.Error(codes.Unimplemented, "grant")
}

func (f *fakeMetadataRootClient) ApplyCapsuleAuthority(ctx context.Context, req *metapb.MetadataRootApplyCapsuleAuthorityRequest, opts ...grpc.CallOption) (*metapb.MetadataRootApplyCapsuleAuthorityResponse, error) {
	if f.applyCapsuleAuthorityFunc != nil {
		return f.applyCapsuleAuthorityFunc(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "capsule authority")
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

	require.True(t, validGrantAct(1))
	require.False(t, validGrantAct(99))
	require.True(t, validCapsuleAuthorityAct(rootproto.CapsuleAuthorityActAcquire))
	require.False(t, validCapsuleAuthorityAct(rootproto.CapsuleAuthorityAct(99)))

	leaderID, ok := leaderHint(metadataRootNotLeaderErrorForTest(23))
	require.True(t, ok)
	require.Equal(t, uint64(23), leaderID)
	_, ok = leaderHint(nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, errMetadataRootNotLeader, map[string]string{
		metaRootReasonMetadata: reasonNotLeader,
	}))
	require.False(t, ok)

	require.NoError(t, waitForReady(context.Background(), nil))
}

func TestClientApplyCapsuleAuthority(t *testing.T) {
	state := rootstate.State{CapsuleAuthorityEpoch: 4}
	grant := rootproto.CapsuleAuthorityGrant{
		GrantID:         "capsule-4",
		EpochID:         4,
		HolderID:        "holder-a",
		Scope:           rootproto.CapsuleAuthorityScope{MountID: "vol", MountKeyID: 7},
		ExpiresUnixNano: 1_000,
	}
	cmd := rootproto.CapsuleAuthorityCommand{
		Kind:            rootproto.CapsuleAuthorityActAcquire,
		HolderID:        grant.HolderID,
		Scope:           grant.Scope,
		ExpiresUnixNano: grant.ExpiresUnixNano,
		NowUnixNano:     100,
	}

	t.Run("success", func(t *testing.T) {
		c := &Client{
			endpoints: []clientEndpoint{{
				id: 1,
				rpc: &fakeMetadataRootClient{
					applyCapsuleAuthorityFunc: func(context.Context, *metapb.MetadataRootApplyCapsuleAuthorityRequest, ...grpc.CallOption) (*metapb.MetadataRootApplyCapsuleAuthorityResponse, error) {
						return &metapb.MetadataRootApplyCapsuleAuthorityResponse{
							State:  metawire.RootStateToProto(state),
							Status: metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_GRANTED,
							Grant:  metawire.RootCapsuleAuthorityGrantToProto(grant),
						}, nil
					},
				},
			}},
			byID: map[uint64]int{1: 0},
		}

		gotState, gotGrant, err := c.ApplyCapsuleAuthority(context.Background(), cmd)
		require.NoError(t, err)
		require.Equal(t, state, gotState)
		require.Equal(t, grant, gotGrant)
	})

	t.Run("held", func(t *testing.T) {
		c := &Client{
			endpoints: []clientEndpoint{{
				id: 1,
				rpc: &fakeMetadataRootClient{
					applyCapsuleAuthorityFunc: func(context.Context, *metapb.MetadataRootApplyCapsuleAuthorityRequest, ...grpc.CallOption) (*metapb.MetadataRootApplyCapsuleAuthorityResponse, error) {
						return &metapb.MetadataRootApplyCapsuleAuthorityResponse{
							State:  metawire.RootStateToProto(state),
							Status: metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD,
						}, nil
					},
				},
			}},
			byID: map[uint64]int{1: 0},
		}

		gotState, _, err := c.ApplyCapsuleAuthority(context.Background(), rootproto.CapsuleAuthorityCommand{
			Kind:     rootproto.CapsuleAuthorityActRetire,
			HolderID: grant.HolderID,
			GrantID:  grant.GrantID,
		})
		require.ErrorIs(t, err, rootstate.ErrPrimacy)
		require.Equal(t, state, gotState)
	})
}

func metadataRootNotLeaderErrorForTest(leaderID uint64) error {
	metadata := map[string]string{metaRootReasonMetadata: reasonNotLeader}
	if leaderID != 0 {
		metadata[leaderIDMetadata] = strconv.FormatUint(leaderID, 10)
	}
	return nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, errMetadataRootNotLeader, metadata)
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
						return nil, metadataRootNotLeaderErrorForTest(2)
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
						return nil, metadataRootNotLeaderErrorForTest(2)
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
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
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
