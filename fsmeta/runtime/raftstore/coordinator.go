// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"strings"
	"sync"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	metadatapb "github.com/feichai0017/NoKV/pb/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultStoreDialTimeout = 3 * time.Second

// CoordinatorClient is the coordinator surface the distributed fsmeta runtime
// needs. The concrete coordinator/client.GRPCClient satisfies it.
type CoordinatorClient interface {
	GetRegionByKey(context.Context, *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error)
	GetStore(context.Context, *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error)
	Tso(context.Context, *coordpb.TsoRequest) (*coordpb.TsoResponse, error)
	AllocID(context.Context, *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error)
	GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
	PublishRootEvent(context.Context, *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error)
}

// CoordinatorRouteProvider resolves fsmeta keys through coordinator route
// truth and dials the selected Rust MetadataPlane endpoint.
type CoordinatorRouteProvider struct {
	coordinator CoordinatorClient
	dialOptions []grpc.DialOption
	dialTimeout time.Duration

	mu          sync.Mutex
	clients     map[string]storeClient
	leaderHints map[uint64]*metapb.RegionPeer
}

type storeClient struct {
	conn   *grpc.ClientConn
	client metadatapb.MetadataPlaneClient
}

type CoordinatorRouteProviderOptions struct {
	DialOptions []grpc.DialOption
	DialTimeout time.Duration
}

func NewCoordinatorRouteProvider(coordinator CoordinatorClient, opts CoordinatorRouteProviderOptions) (*CoordinatorRouteProvider, error) {
	if coordinator == nil {
		return nil, errCoordinatorRequired
	}
	dialOptions := opts.DialOptions
	if len(dialOptions) == 0 {
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	timeout := opts.DialTimeout
	if timeout <= 0 {
		timeout = defaultStoreDialTimeout
	}
	return &CoordinatorRouteProvider{
		coordinator: coordinator,
		dialOptions: append([]grpc.DialOption(nil), dialOptions...),
		dialTimeout: timeout,
		clients:     make(map[string]storeClient),
		leaderHints: make(map[uint64]*metapb.RegionPeer),
	}, nil
}

func (p *CoordinatorRouteProvider) RouteForKey(ctx context.Context, key []byte) (MetadataRoute, error) {
	if p == nil || p.coordinator == nil {
		return MetadataRoute{}, errRouteProviderRequired
	}
	resp, err := p.coordinator.GetRegionByKey(ctx, &coordpb.GetRegionByKeyRequest{
		Key:       cloneBytes(key),
		Freshness: coordpb.Freshness_FRESHNESS_STRONG,
	})
	if err != nil {
		return MetadataRoute{}, err
	}
	if resp.GetNotFound() || resp.GetRegionDescriptor() == nil {
		return MetadataRoute{}, nokverrors.New(nokverrors.KindRouteUnavailable, "fsmeta/runtime/raftstore: no region route for metadata key")
	}
	peers := servingPeerCandidates(resp.GetRegionDescriptor(), resp.GetLeaderPeer(), p.leaderHint(resp.GetRegionDescriptor().GetRegionId()))
	if len(peers) == 0 {
		return MetadataRoute{}, nokverrors.New(nokverrors.KindRouteUnavailable, "fsmeta/runtime/raftstore: metadata region has no serving peer")
	}
	var lastErr error
	for _, peer := range peers {
		storeResp, err := p.coordinator.GetStore(ctx, &coordpb.GetStoreRequest{StoreId: peer.GetStoreId()})
		if err != nil {
			lastErr = err
			continue
		}
		store := storeResp.GetStore()
		if storeResp.GetNotFound() || store == nil || store.GetState() != coordpb.StoreState_STORE_STATE_UP || strings.TrimSpace(store.GetClientAddr()) == "" {
			continue
		}
		client, err := p.clientForStore(ctx, store.GetClientAddr())
		if err != nil {
			lastErr = err
			continue
		}
		return MetadataRoute{
			Context: &metadatapb.MetadataContext{
				RegionId:        resp.GetRegionDescriptor().GetRegionId(),
				RegionEpoch:     resp.GetRegionDescriptor().GetEpoch(),
				Peer:            peer,
				ReadConsistency: metadatapb.ReadConsistency_READ_CONSISTENCY_STRONG,
				ReadPreference:  metadatapb.ReadPreference_READ_PREFERENCE_LEADER_ONLY,
			},
			Client: client,
		}, nil
	}
	if lastErr != nil {
		return MetadataRoute{}, lastErr
	}
	return MetadataRoute{}, nokverrors.New(nokverrors.KindRouteUnavailable, "fsmeta/runtime/raftstore: metadata region has no routable peer")
}

func (p *CoordinatorRouteProvider) ObserveRegionError(_ context.Context, _ []byte, _ MetadataRoute, err *errorpb.RegionError) {
	if p == nil || err == nil || err.GetNotLeader() == nil || err.GetNotLeader().GetLeader() == nil {
		return
	}
	notLeader := err.GetNotLeader()
	leader := cloneRegionPeer(notLeader.GetLeader())
	if leader == nil {
		return
	}
	p.mu.Lock()
	p.leaderHints[notLeader.GetRegionId()] = leader
	p.mu.Unlock()
}

func (p *CoordinatorRouteProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	clients := p.clients
	p.clients = make(map[string]storeClient)
	p.mu.Unlock()
	var first error
	for _, client := range clients {
		if client.conn == nil {
			continue
		}
		if err := client.conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (p *CoordinatorRouteProvider) leaderHint(regionID uint64) *metapb.RegionPeer {
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneRegionPeer(p.leaderHints[regionID])
}

func (p *CoordinatorRouteProvider) clientForStore(ctx context.Context, addr string) (metadatapb.MetadataPlaneClient, error) {
	target := normalizeStoreTarget(addr)
	p.mu.Lock()
	if cached, ok := p.clients[target]; ok {
		p.mu.Unlock()
		return cached.client, nil
	}
	p.mu.Unlock()
	dialCtx, cancel := context.WithTimeout(ctx, p.dialTimeout)
	defer cancel()
	conn, err := grpc.NewClient(target, p.dialOptions...)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if !conn.WaitForStateChange(dialCtx, state) {
			_ = conn.Close()
			return nil, dialCtx.Err()
		}
	}
	client := metadatapb.NewMetadataPlaneClient(conn)
	p.mu.Lock()
	if cached, ok := p.clients[target]; ok {
		p.mu.Unlock()
		_ = conn.Close()
		return cached.client, nil
	}
	p.clients[target] = storeClient{conn: conn, client: client}
	p.mu.Unlock()
	return client, nil
}

func normalizeStoreTarget(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" || strings.Contains(addr, "://") {
		return addr
	}
	return "dns:///" + addr
}

func servingPeerCandidates(desc *metapb.RegionDescriptor, leader, hint *metapb.RegionPeer) []*metapb.RegionPeer {
	var out []*metapb.RegionPeer
	add := func(peer *metapb.RegionPeer) {
		if !descriptorHasPeer(desc, peer) || peerInCandidateList(out, peer) {
			return
		}
		out = append(out, cloneRegionPeer(peer))
	}
	add(leader)
	add(hint)
	for _, peer := range desc.GetPeers() {
		add(peer)
	}
	return out
}

func peerInCandidateList(peers []*metapb.RegionPeer, peer *metapb.RegionPeer) bool {
	if peer == nil {
		return false
	}
	for _, candidate := range peers {
		if candidate.GetStoreId() == peer.GetStoreId() && candidate.GetPeerId() == peer.GetPeerId() {
			return true
		}
	}
	return false
}

func descriptorHasPeer(desc *metapb.RegionDescriptor, peer *metapb.RegionPeer) bool {
	if desc == nil || peer == nil || peer.GetStoreId() == 0 || peer.GetPeerId() == 0 {
		return false
	}
	for _, candidate := range desc.GetPeers() {
		if candidate.GetStoreId() == peer.GetStoreId() && candidate.GetPeerId() == peer.GetPeerId() {
			return true
		}
	}
	return false
}

func cloneRegionPeer(peer *metapb.RegionPeer) *metapb.RegionPeer {
	if peer == nil {
		return nil
	}
	return &metapb.RegionPeer{StoreId: peer.GetStoreId(), PeerId: peer.GetPeerId()}
}

type CoordinatorTimestampSource struct {
	coordinator CoordinatorClient
}

func NewCoordinatorTimestampSource(coordinator CoordinatorClient) (*CoordinatorTimestampSource, error) {
	if coordinator == nil {
		return nil, errCoordinatorRequired
	}
	return &CoordinatorTimestampSource{coordinator: coordinator}, nil
}

func (s *CoordinatorTimestampSource) ReserveTimestamp(ctx context.Context, count uint64) (uint64, error) {
	if s == nil || s.coordinator == nil {
		return 0, errTimestampSourceRequired
	}
	resp, err := s.coordinator.Tso(ctx, &coordpb.TsoRequest{Count: count})
	if err != nil {
		return 0, err
	}
	return resp.GetTimestamp(), nil
}
