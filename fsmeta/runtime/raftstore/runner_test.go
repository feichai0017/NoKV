// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	metadatapb "github.com/feichai0017/NoKV/pb/metadata"
)

func TestRunnerCommitMetadataSendsMetadataCommand(t *testing.T) {
	var got *metadatapb.MetadataCommitRequest
	client := &fakeMetadataPlaneClient{
		commitMetadata: func(_ context.Context, req *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error) {
			got = req
			return &metadatapb.MetadataCommitResponse{
				Result: &metadatapb.MetadataCommitResult{
					CommitVersion:    12,
					RegionId:         7,
					Term:             3,
					Index:            41,
					AppliedMutations: 1,
				},
			}, nil
		},
	}
	runner := newTestRunner(t, client)

	result, err := runner.CommitMetadata(context.Background(), backend.MetadataCommand{
		RequestID:     []byte("req-1"),
		Mount:         "mount-a",
		MountKeyID:    9,
		PrimaryKey:    []byte("k"),
		ReadVersion:   10,
		CommitVersion: 11,
		Predicates: []*backend.Predicate{
			{Key: []byte("k"), Kind: backend.PredicateNotExists, ReadVersion: 10},
		},
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: []byte("k"), Value: []byte("v")},
		},
		WatchKeys: [][]byte{[]byte("watch/k")},
	})
	require.NoError(t, err)
	require.Equal(t, backend.MetadataCommitResult{
		CommitVersion:    12,
		RegionID:         7,
		Term:             3,
		Index:            41,
		AppliedMutations: 1,
	}, result)
	require.NotNil(t, got)
	require.Equal(t, uint64(7), got.GetContext().GetRegionId())
	require.Equal(t, []byte("req-1"), got.GetCommand().GetRequestId())
	require.Equal(t, "mount-a", got.GetCommand().GetMount())
	require.Equal(t, uint64(9), got.GetCommand().GetMountKeyId())
	require.Equal(t, uint64(10), got.GetCommand().GetReadVersion())
	require.Equal(t, uint64(11), got.GetCommand().GetCommitVersion())
	require.Equal(t, []byte("k"), got.GetCommand().GetPrimaryKey())
	require.Len(t, got.GetCommand().GetPredicates(), 1)
	require.Len(t, got.GetCommand().GetMutations(), 1)
	require.Equal(t, []byte("watch/k"), got.GetCommand().GetWatchKeys()[0])
}

func TestRunnerBatchGetMapsResponseByRequestKey(t *testing.T) {
	client := &fakeMetadataPlaneClient{
		batchGet: func(_ context.Context, req *metadatapb.MetadataBatchGetRequest) (*metadatapb.MetadataBatchGetResponse, error) {
			require.Len(t, req.GetRequests(), 2)
			return &metadatapb.MetadataBatchGetResponse{
				Responses: []*metadatapb.MetadataGetResponse{
					{Kv: &metadatapb.MetadataKV{Value: []byte("va")}},
					{NotFound: true},
				},
			}, nil
		},
	}
	runner := newTestRunner(t, client)

	values, err := runner.BatchGet(context.Background(), [][]byte{[]byte("a"), []byte("b")}, 5)
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"a": []byte("va")}, values)
}

func TestRunnerMapsRegionErrorToRetryableKind(t *testing.T) {
	client := &fakeMetadataPlaneClient{
		get: func(context.Context, *metadatapb.MetadataGetRequest) (*metadatapb.MetadataGetResponse, error) {
			return &metadatapb.MetadataGetResponse{
				RegionError: &errorpb.RegionError{
					NotLeader: &errorpb.NotLeader{RegionId: 7},
				},
			}, nil
		},
	}
	runner := newTestRunner(t, client)

	_, _, err := runner.Get(context.Background(), []byte("k"), 10)
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindNotLeader))
	require.True(t, nokverrors.Retryable(err))
}

func TestRunnerMapsMetadataKeyError(t *testing.T) {
	client := &fakeMetadataPlaneClient{
		commitMetadata: func(context.Context, *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error) {
			return &metadatapb.MetadataCommitResponse{
				Error: &metadatapb.MetadataKeyError{
					AlreadyExists: &metadatapb.MetadataKeyAlreadyExists{Key: []byte("k")},
				},
			}, nil
		},
	}
	runner := newTestRunner(t, client)

	_, err := runner.CommitMetadata(context.Background(), backend.MetadataCommand{
		PrimaryKey:  []byte("k"),
		ReadVersion: 1,
		Mutations:   []*backend.Mutation{{Op: backend.MutationPut, Key: []byte("k"), Value: []byte("v")}},
		Predicates:  []*backend.Predicate{{Key: []byte("k"), Kind: backend.PredicateNotExists, ReadVersion: 1}},
		WatchKeys:   [][]byte{[]byte("k")},
	})
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindAlreadyExists))
}

func TestRunnerCommitMetadataRetriesAfterNotLeaderRouteError(t *testing.T) {
	var firstAttempts int
	var secondAttempts int
	firstClient := &fakeMetadataPlaneClient{
		commitMetadata: func(context.Context, *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error) {
			firstAttempts++
			return &metadatapb.MetadataCommitResponse{
				RegionError: &errorpb.RegionError{
					NotLeader: &errorpb.NotLeader{
						RegionId: 7,
						Leader:   &metapb.RegionPeer{StoreId: 2, PeerId: 22},
					},
				},
			}, nil
		},
	}
	secondClient := &fakeMetadataPlaneClient{
		commitMetadata: func(_ context.Context, req *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error) {
			secondAttempts++
			require.Equal(t, []byte("req-retry"), req.GetCommand().GetRequestId())
			require.Equal(t, []byte("primary"), req.GetCommand().GetPrimaryKey())
			require.Equal(t, uint64(2), req.GetContext().GetPeer().GetStoreId())
			return &metadatapb.MetadataCommitResponse{
				Result: &metadatapb.MetadataCommitResult{
					CommitVersion:    12,
					RegionId:         7,
					Term:             3,
					Index:            44,
					AppliedMutations: 1,
				},
			}, nil
		},
	}
	routes := &notLeaderRetryRouteProvider{
		initial: MetadataRoute{
			Context: &metadatapb.MetadataContext{
				RegionId: 7,
				Peer:     &metapb.RegionPeer{StoreId: 1, PeerId: 11},
			},
			Client: firstClient,
		},
		leader: MetadataRoute{
			Context: &metadatapb.MetadataContext{
				RegionId: 7,
				Peer:     &metapb.RegionPeer{StoreId: 2, PeerId: 22},
			},
			Client: secondClient,
		},
	}
	runner, err := NewRunner(routes, newMonotonicTimestampSource(1))
	require.NoError(t, err)

	result, err := runner.CommitMetadata(context.Background(), backend.MetadataCommand{
		RequestID:   []byte("req-retry"),
		PrimaryKey:  []byte("primary"),
		ReadVersion: 10,
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: []byte("primary"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(12), result.CommitVersion)
	require.Equal(t, 1, firstAttempts)
	require.Equal(t, 1, secondAttempts)
	require.Equal(t, 2, routes.routeCalls)
	require.Equal(t, 1, routes.observedNotLeader)
	require.Equal(t, []byte("primary"), routes.observedKey)
}

func TestRunnerCommitMetadataRetriesAfterTransportRouteFailure(t *testing.T) {
	var firstAttempts int
	var secondAttempts int
	firstClient := &fakeMetadataPlaneClient{
		commitMetadata: func(context.Context, *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error) {
			firstAttempts++
			return nil, status.Error(codes.Unavailable, "cached leader connection is down")
		},
	}
	secondClient := &fakeMetadataPlaneClient{
		commitMetadata: func(_ context.Context, req *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error) {
			secondAttempts++
			require.Equal(t, []byte("req-transport-retry"), req.GetCommand().GetRequestId())
			require.Equal(t, uint64(2), req.GetContext().GetPeer().GetStoreId())
			return &metadatapb.MetadataCommitResponse{
				Result: &metadatapb.MetadataCommitResult{
					CommitVersion:    14,
					RegionId:         7,
					Term:             4,
					Index:            46,
					AppliedMutations: 1,
				},
			}, nil
		},
	}
	routes := &transportRetryRouteProvider{
		initial: MetadataRoute{
			Context: &metadatapb.MetadataContext{
				RegionId: 7,
				Peer:     &metapb.RegionPeer{StoreId: 1, PeerId: 11},
			},
			StoreAddr: "store-1",
			Client:    firstClient,
		},
		next: MetadataRoute{
			Context: &metadatapb.MetadataContext{
				RegionId: 7,
				Peer:     &metapb.RegionPeer{StoreId: 2, PeerId: 22},
			},
			StoreAddr: "store-2",
			Client:    secondClient,
		},
	}
	runner, err := NewRunner(routes, newMonotonicTimestampSource(1))
	require.NoError(t, err)

	result, err := runner.CommitMetadata(context.Background(), backend.MetadataCommand{
		RequestID:   []byte("req-transport-retry"),
		PrimaryKey:  []byte("primary"),
		ReadVersion: 10,
		Mutations: []*backend.Mutation{
			{Op: backend.MutationPut, Key: []byte("primary"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(14), result.CommitVersion)
	require.Equal(t, 1, firstAttempts)
	require.Equal(t, 1, secondAttempts)
	require.Equal(t, 2, routes.routeCalls)
	require.Equal(t, 1, routes.observedFailures)
	require.Equal(t, []byte("primary"), routes.observedKey)
}

func TestCoordinatorRouteProviderUsesLeaderHintFromCoordinator(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 3},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 11},
					{StoreId: 2, PeerId: 22},
				},
			},
			LeaderPeer: &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			2: {
				Store: &coordpb.StoreInfo{
					StoreId:    2,
					ClientAddr: "passthrough:///store-2",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
		},
	}
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	route, err := provider.RouteForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, uint64(9), route.Context.GetRegionId())
	require.Equal(t, uint64(2), route.Context.GetPeer().GetStoreId())
	require.NotNil(t, route.Client)
}

func TestCoordinatorRouteProviderLearnsNotLeaderHint(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 3},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 11},
					{StoreId: 2, PeerId: 22},
				},
			},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			2: {
				Store: &coordpb.StoreInfo{
					StoreId:    2,
					ClientAddr: "passthrough:///store-2",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
		},
	}
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	provider.ObserveRegionError(context.Background(), []byte("k"), MetadataRoute{}, &errorpb.RegionError{
		NotLeader: &errorpb.NotLeader{
			RegionId: 9,
			Leader:   &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
	})

	route, err := provider.RouteForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), route.Context.GetPeer().GetStoreId())
}

func TestCoordinatorRouteProviderPrefersNotLeaderHintOverStaleCoordinatorLeader(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 3},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 11},
					{StoreId: 2, PeerId: 22},
				},
			},
			LeaderPeer: &metapb.RegionPeer{StoreId: 1, PeerId: 11},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			1: {
				Store: &coordpb.StoreInfo{
					StoreId:    1,
					ClientAddr: "passthrough:///store-1",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
			2: {
				Store: &coordpb.StoreInfo{
					StoreId:    2,
					ClientAddr: "passthrough:///store-2",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
		},
	}
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	provider.ObserveRegionError(context.Background(), []byte("k"), MetadataRoute{}, &errorpb.RegionError{
		NotLeader: &errorpb.NotLeader{
			RegionId: 9,
			Leader:   &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
	})

	route, err := provider.RouteForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), route.Context.GetPeer().GetStoreId())
	require.Equal(t, uint64(22), route.Context.GetPeer().GetPeerId())
}

func TestCoordinatorRouteProviderCachesResolvedRoute(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := fakeRouteCoordinator()
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	first, err := provider.RouteForKey(context.Background(), []byte("k1"))
	require.NoError(t, err)
	second, err := provider.RouteForKey(context.Background(), []byte("k2"))
	require.NoError(t, err)
	require.Equal(t, first.Context.GetRegionId(), second.Context.GetRegionId())
	require.Equal(t, uint64(1), coordinator.regionCallCount())
	require.Equal(t, uint64(1), coordinator.storeCallCount(2))
}

func TestCoordinatorRouteProviderInvalidatesCachedRouteOnNotLeader(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 3},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 11},
					{StoreId: 2, PeerId: 22},
				},
			},
			LeaderPeer: &metapb.RegionPeer{StoreId: 1, PeerId: 11},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			1: {
				Store: &coordpb.StoreInfo{
					StoreId:    1,
					ClientAddr: "passthrough:///store-1",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
			2: {
				Store: &coordpb.StoreInfo{
					StoreId:    2,
					ClientAddr: "passthrough:///store-2",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
		},
	}
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	route, err := provider.RouteForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), route.Context.GetPeer().GetStoreId())
	provider.ObserveRegionError(context.Background(), []byte("k"), route, &errorpb.RegionError{
		NotLeader: &errorpb.NotLeader{
			RegionId: 9,
			Leader:   &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
	})

	route, err = provider.RouteForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), route.Context.GetPeer().GetStoreId())
	require.Equal(t, uint64(2), coordinator.regionCallCount())
	require.Equal(t, uint64(1), coordinator.storeCallCount(1))
	require.Equal(t, uint64(1), coordinator.storeCallCount(2))
}

func TestCoordinatorRouteProviderIgnoresLeaderHintRemovedFromDescriptor(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 3, ConfVersion: 4},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 11},
				},
			},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			1: {
				Store: &coordpb.StoreInfo{
					StoreId:    1,
					ClientAddr: "passthrough:///store-1",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
		},
	}
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	provider.ObserveRegionError(context.Background(), []byte("k"), MetadataRoute{}, &errorpb.RegionError{
		NotLeader: &errorpb.NotLeader{
			RegionId: 9,
			Leader:   &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
	})

	route, err := provider.RouteForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), route.Context.GetPeer().GetStoreId())
	require.Equal(t, uint64(11), route.Context.GetPeer().GetPeerId())
}

func TestCoordinatorRouteProviderFallsBackWhenLeaderStoreIsDown(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 3},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 11},
					{StoreId: 2, PeerId: 22},
				},
			},
			LeaderPeer: &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			1: {
				Store: &coordpb.StoreInfo{
					StoreId:    1,
					ClientAddr: "passthrough:///store-1",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
			2: {
				Store: &coordpb.StoreInfo{
					StoreId:    2,
					ClientAddr: "passthrough:///store-2",
					State:      coordpb.StoreState_STORE_STATE_DOWN,
				},
			},
		},
	}
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	route, err := provider.RouteForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), route.Context.GetPeer().GetStoreId())
	require.Equal(t, uint64(11), route.Context.GetPeer().GetPeerId())
}

func TestCoordinatorRouteProviderRejectsRegionWithoutRoutablePeer(t *testing.T) {
	coordinator := &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 3},
				Peers: []*metapb.RegionPeer{
					{StoreId: 1, PeerId: 11},
					{StoreId: 2, PeerId: 22},
				},
			},
			LeaderPeer: &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			1: {Store: &coordpb.StoreInfo{StoreId: 1, State: coordpb.StoreState_STORE_STATE_DOWN}},
			2: {Store: &coordpb.StoreInfo{StoreId: 2, State: coordpb.StoreState_STORE_STATE_DOWN}},
		},
	}
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	_, err = provider.RouteForKey(context.Background(), []byte("k"))
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindRouteUnavailable))
}

func TestMountResolverRejectsUnregisteredMount(t *testing.T) {
	coordinator := fakeRouteCoordinator()
	coordinator.mount = &coordpb.GetMountResponse{NotFound: true}
	resolver, err := NewMountResolver(coordinator)
	require.NoError(t, err)

	_, err = resolver.ResolveMount(context.Background(), "vol")
	require.ErrorIs(t, err, model.ErrMountNotRegistered)
}

func TestMountResolverRejectsRetiredMount(t *testing.T) {
	coordinator := fakeRouteCoordinator()
	coordinator.mount = &coordpb.GetMountResponse{
		Mount: &coordpb.MountInfo{
			MountId:       "vol",
			MountKeyId:    1,
			RootInode:     uint64(model.RootInode),
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_RETIRED,
		},
	}
	resolver, err := NewMountResolver(coordinator)
	require.NoError(t, err)

	_, err = resolver.ResolveMount(context.Background(), "vol")
	require.ErrorIs(t, err, model.ErrMountRetired)
}

func TestWatcherStreamsMetadataApplyEvents(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	watchKey := []byte("watched-key")
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{
		watchApply: func(req *metadatapb.MetadataWatchApplyRequest, stream grpc.ServerStreamingServer[metadatapb.MetadataWatchApplyResponse]) error {
			require.NotEmpty(t, req.GetKeyPrefix())
			return stream.Send(&metadatapb.MetadataWatchApplyResponse{
				Event: &metadatapb.MetadataApplyWatchEvent{
					RegionId:      9,
					Term:          2,
					Index:         7,
					Source:        metadatapb.MetadataApplyWatchEventSource_METADATA_APPLY_WATCH_EVENT_SOURCE_COMMIT,
					CommitVersion: 99,
					Keys:          [][]byte{watchKey},
				},
			})
		},
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := fakeRouteCoordinator()
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	mounts, err := NewMountResolver(coordinator)
	require.NoError(t, err)
	watcher, err := NewWatcher(provider, mounts)
	require.NoError(t, err)

	sub, err := watcher.Subscribe(context.Background(), observe.WatchRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
	})
	require.NoError(t, err)
	defer sub.Close()
	select {
	case evt := <-sub.Events():
		require.Equal(t, observe.WatchCursor{RegionID: 9, Term: 2, Index: 7}, evt.Cursor)
		require.Equal(t, uint64(99), evt.CommitVersion)
		require.Equal(t, observe.WatchEventSourceCommit, evt.Source)
		require.Equal(t, watchKey, evt.Key)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for metadata apply watch event")
	}
}

func TestWatcherSendsResumeCursorToMetadataPlane(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	seen := make(chan *metadatapb.MetadataWatchApplyRequest, 1)
	metadatapb.RegisterMetadataPlaneServer(server, &fakeMetadataPlaneServer{
		watchApply: func(req *metadatapb.MetadataWatchApplyRequest, stream grpc.ServerStreamingServer[metadatapb.MetadataWatchApplyResponse]) error {
			seen <- req
			return nil
		},
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	coordinator := fakeRouteCoordinator()
	provider, err := NewCoordinatorRouteProvider(coordinator, CoordinatorRouteProviderOptions{
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	mounts, err := NewMountResolver(coordinator)
	require.NoError(t, err)
	watcher, err := NewWatcher(provider, mounts)
	require.NoError(t, err)
	sub, err := watcher.Subscribe(context.Background(), observe.WatchRequest{
		Mount:        "vol",
		RootInode:    model.RootInode,
		ResumeCursor: observe.WatchCursor{RegionID: 7, Term: 1, Index: 10},
	})
	require.NoError(t, err)
	defer sub.Close()

	select {
	case req := <-seen:
		require.Equal(t, uint64(7), req.GetResumeRegionId())
		require.Equal(t, uint64(1), req.GetResumeTerm())
		require.Equal(t, uint64(10), req.GetResumeIndex())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for metadata apply watch request")
	}
}

func TestSnapshotPublisherPublishesAndRetiresRootEvents(t *testing.T) {
	coordinator := fakeRouteCoordinator()
	var committed []*metadatapb.MetadataCommand
	runner := newTestRunner(t, &fakeMetadataPlaneClient{
		commitMetadata: func(_ context.Context, req *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error) {
			committed = append(committed, proto.Clone(req.GetCommand()).(*metadatapb.MetadataCommand))
			return &metadatapb.MetadataCommitResponse{Result: &metadatapb.MetadataCommitResult{
				CommitVersion:    req.GetCommand().GetCommitVersion(),
				RegionId:         7,
				AppliedMutations: uint64(len(req.GetCommand().GetMutations())),
			}}, nil
		},
	})
	publisher, err := NewSnapshotPublisher(coordinator, runner)
	require.NoError(t, err)
	token := model.SnapshotSubtreeToken{
		Mount:       "vol",
		MountKeyID:  1,
		RootInode:   model.RootInode,
		ReadVersion: 42,
	}

	require.NoError(t, publisher.PublishSnapshotSubtree(context.Background(), token))
	require.NoError(t, publisher.RetireSnapshotSubtree(context.Background(), token))
	require.Len(t, coordinator.published, 2)
	published := metawire.RootEventFromProto(coordinator.published[0])
	retired := metawire.RootEventFromProto(coordinator.published[1])
	require.Equal(t, rootevent.KindSnapshotEpochPublished, published.Kind)
	require.Equal(t, rootevent.KindSnapshotEpochRetired, retired.Kind)
	require.Equal(t, rootevent.SnapshotEpochID("vol", uint64(model.RootInode), 42), published.SnapshotEpoch.SnapshotID)
	require.Equal(t, published.SnapshotEpoch.SnapshotID, retired.SnapshotEpoch.SnapshotID)
	require.Len(t, committed, 2)
	require.Len(t, committed[0].GetMutations(), 1)
	require.Equal(t, metadatapb.MetadataFamily_METADATA_FAMILY_SNAPSHOT, committed[0].GetMutations()[0].GetKeyFamily())
	require.Equal(t, uint64(42), committed[0].GetMutations()[0].GetRetentionPinVersion())
	require.Len(t, committed[1].GetMutations(), 1)
	require.Equal(t, metadatapb.MetadataMutation_DELETE, committed[1].GetMutations()[0].GetOp())
	require.Zero(t, committed[1].GetMutations()[0].GetRetentionPinVersion())
	stats := publisher.Stats()
	require.Equal(t, uint64(1), stats["publish_total"])
	require.Equal(t, uint64(1), stats["retire_total"])
	require.Equal(t, uint64(0), stats["publish_error_total"])
	require.Equal(t, uint64(0), stats["retire_error_total"])
	require.Equal(t, uint64(0), stats["root_rejected_total"])
	require.Equal(t, "metadata_snapshot_pin+root_snapshot_epoch", stats["durability_authority"])
}

func newTestRunner(t *testing.T, client metadatapb.MetadataPlaneClient) *Runner {
	t.Helper()
	runner, err := NewRunner(staticRouteProvider{
		Context: &metadatapb.MetadataContext{RegionId: 7},
		Client:  client,
	}, newMonotonicTimestampSource(1))
	require.NoError(t, err)
	return runner
}

type staticRouteProvider struct {
	Context *metadatapb.MetadataContext
	Client  metadatapb.MetadataPlaneClient
}

func (p staticRouteProvider) RouteForKey(context.Context, []byte) (MetadataRoute, error) {
	if p.Context == nil || p.Context.GetRegionId() == 0 {
		return MetadataRoute{}, errRouteProviderRequired
	}
	if p.Client == nil {
		return MetadataRoute{}, errClientRequired
	}
	return MetadataRoute{
		Context: cloneMetadataContext(p.Context),
		Client:  p.Client,
	}, nil
}

type monotonicTimestampSource struct {
	mu   sync.Mutex
	next uint64
}

func newMonotonicTimestampSource(first uint64) *monotonicTimestampSource {
	if first == 0 {
		first = 1
	}
	return &monotonicTimestampSource{next: first}
}

func (s *monotonicTimestampSource) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errInvalidMetadataCommand
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.next
	s.next += count
	return first, nil
}

type fakeMetadataPlaneClient struct {
	get            func(context.Context, *metadatapb.MetadataGetRequest) (*metadatapb.MetadataGetResponse, error)
	batchGet       func(context.Context, *metadatapb.MetadataBatchGetRequest) (*metadatapb.MetadataBatchGetResponse, error)
	scan           func(context.Context, *metadatapb.MetadataScanRequest) (*metadatapb.MetadataScanResponse, error)
	commitMetadata func(context.Context, *metadatapb.MetadataCommitRequest) (*metadatapb.MetadataCommitResponse, error)
}

func (c *fakeMetadataPlaneClient) Get(ctx context.Context, req *metadatapb.MetadataGetRequest, _ ...grpc.CallOption) (*metadatapb.MetadataGetResponse, error) {
	if c.get == nil {
		return &metadatapb.MetadataGetResponse{}, nil
	}
	return c.get(ctx, req)
}

func (c *fakeMetadataPlaneClient) BatchGet(ctx context.Context, req *metadatapb.MetadataBatchGetRequest, _ ...grpc.CallOption) (*metadatapb.MetadataBatchGetResponse, error) {
	if c.batchGet == nil {
		return &metadatapb.MetadataBatchGetResponse{}, nil
	}
	return c.batchGet(ctx, req)
}

func (c *fakeMetadataPlaneClient) Scan(ctx context.Context, req *metadatapb.MetadataScanRequest, _ ...grpc.CallOption) (*metadatapb.MetadataScanResponse, error) {
	if c.scan == nil {
		return &metadatapb.MetadataScanResponse{}, nil
	}
	return c.scan(ctx, req)
}

func (c *fakeMetadataPlaneClient) CommitMetadata(ctx context.Context, req *metadatapb.MetadataCommitRequest, _ ...grpc.CallOption) (*metadatapb.MetadataCommitResponse, error) {
	if c.commitMetadata == nil {
		return &metadatapb.MetadataCommitResponse{}, nil
	}
	return c.commitMetadata(ctx, req)
}

func (c *fakeMetadataPlaneClient) WatchApply(context.Context, *metadatapb.MetadataWatchApplyRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[metadatapb.MetadataWatchApplyResponse], error) {
	return nil, nil
}

type fakeMetadataPlaneServer struct {
	metadatapb.UnimplementedMetadataPlaneServer
	watchApply func(*metadatapb.MetadataWatchApplyRequest, grpc.ServerStreamingServer[metadatapb.MetadataWatchApplyResponse]) error
}

func (s *fakeMetadataPlaneServer) WatchApply(req *metadatapb.MetadataWatchApplyRequest, stream grpc.ServerStreamingServer[metadatapb.MetadataWatchApplyResponse]) error {
	if s.watchApply == nil {
		return nil
	}
	return s.watchApply(req, stream)
}

type fakeCoordinatorClient struct {
	mu             sync.Mutex
	region         *coordpb.GetRegionByKeyResponse
	stores         map[uint64]*coordpb.GetStoreResponse
	mount          *coordpb.GetMountResponse
	published      []*metapb.RootEvent
	regionCalls    uint64
	storeCallsByID map[uint64]uint64
	nextAllocID    uint64
	allocCalls     uint64
	allocCounts    []uint64
}

func (c *fakeCoordinatorClient) GetRegionByKey(context.Context, *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.regionCalls++
	return c.region, nil
}

func (c *fakeCoordinatorClient) GetStore(_ context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.storeCallsByID == nil {
		c.storeCallsByID = make(map[uint64]uint64)
	}
	c.storeCallsByID[req.GetStoreId()]++
	return c.stores[req.GetStoreId()], nil
}

func (c *fakeCoordinatorClient) Tso(context.Context, *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	return &coordpb.TsoResponse{Timestamp: 10, Count: 1}, nil
}

func (c *fakeCoordinatorClient) AllocID(_ context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	first := c.nextAllocID
	if first == 0 {
		first = 100
	}
	c.nextAllocID = first + count
	c.allocCalls++
	c.allocCounts = append(c.allocCounts, count)
	return &coordpb.AllocIDResponse{FirstId: first, Count: count}, nil
}

func (c *fakeCoordinatorClient) GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mount != nil {
		return c.mount, nil
	}
	return &coordpb.GetMountResponse{
		Mount: &coordpb.MountInfo{
			MountId:       "vol",
			MountKeyId:    1,
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		},
	}, nil
}

func (c *fakeCoordinatorClient) PublishRootEvent(_ context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.published = append(c.published, req.GetEvent())
	return &coordpb.PublishRootEventResponse{Accepted: true}, nil
}

func (c *fakeCoordinatorClient) regionCallCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.regionCalls
}

func (c *fakeCoordinatorClient) storeCallCount(storeID uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.storeCallsByID[storeID]
}

func (c *fakeCoordinatorClient) allocCallCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allocCalls
}

func (c *fakeCoordinatorClient) allocRequestCounts() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.allocCounts...)
}

func fakeRouteCoordinator() *fakeCoordinatorClient {
	return &fakeCoordinatorClient{
		region: &coordpb.GetRegionByKeyResponse{
			RegionDescriptor: &metapb.RegionDescriptor{
				RegionId: 9,
				Epoch:    &metapb.RegionEpoch{Version: 2, ConfVersion: 3},
				Peers:    []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}},
			},
			LeaderPeer: &metapb.RegionPeer{StoreId: 2, PeerId: 22},
		},
		stores: map[uint64]*coordpb.GetStoreResponse{
			2: {
				Store: &coordpb.StoreInfo{
					StoreId:    2,
					ClientAddr: "passthrough:///store-2",
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			},
		},
	}
}

type notLeaderRetryRouteProvider struct {
	initial MetadataRoute
	leader  MetadataRoute

	routeCalls        int
	observedNotLeader int
	observedKey       []byte
}

func (p *notLeaderRetryRouteProvider) RouteForKey(context.Context, []byte) (MetadataRoute, error) {
	p.routeCalls++
	if p.observedNotLeader > 0 {
		return p.leader, nil
	}
	return p.initial, nil
}

func (p *notLeaderRetryRouteProvider) ObserveRegionError(_ context.Context, key []byte, _ MetadataRoute, err *errorpb.RegionError) {
	if err.GetNotLeader() == nil {
		return
	}
	p.observedNotLeader++
	p.observedKey = cloneBytes(key)
}

type transportRetryRouteProvider struct {
	initial MetadataRoute
	next    MetadataRoute

	routeCalls       int
	observedFailures int
	observedKey      []byte
}

func (p *transportRetryRouteProvider) RouteForKey(context.Context, []byte) (MetadataRoute, error) {
	p.routeCalls++
	if p.observedFailures > 0 {
		return p.next, nil
	}
	return p.initial, nil
}

func (p *transportRetryRouteProvider) ObserveRouteFailure(_ context.Context, key []byte, _ MetadataRoute, err error) {
	if err == nil {
		return
	}
	p.observedFailures++
	p.observedKey = cloneBytes(key)
}
