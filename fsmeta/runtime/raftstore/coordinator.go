// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"bytes"
	"context"
	"fmt"
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
	routeCache  []cachedMetadataRoute
	leaderHints map[uint64]*metapb.RegionPeer
	failedPeers map[uint64]map[string]time.Time
}

type storeClient struct {
	conn   *grpc.ClientConn
	client metadatapb.MetadataPlaneClient
}

type cachedMetadataRoute struct {
	startKey  []byte
	endKey    []byte
	regionID  uint64
	context   *metadatapb.MetadataContext
	storeAddr string
	client    metadatapb.MetadataPlaneClient
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
		failedPeers: make(map[uint64]map[string]time.Time),
	}, nil
}

func (p *CoordinatorRouteProvider) RouteForKey(ctx context.Context, key []byte) (MetadataRoute, error) {
	if p == nil || p.coordinator == nil {
		return MetadataRoute{}, errRouteProviderRequired
	}
	if route, ok := p.cachedRouteForKey(key, time.Now()); ok {
		return route, nil
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
	regionID := resp.GetRegionDescriptor().GetRegionId()
	now := time.Now()
	var lastErr error
	for _, peer := range peers {
		if p.peerFailureActive(regionID, peer, now) {
			continue
		}
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
		route := MetadataRoute{
			Context: &metadatapb.MetadataContext{
				RegionId:        regionID,
				RegionEpoch:     resp.GetRegionDescriptor().GetEpoch(),
				Peer:            peer,
				ReadConsistency: metadatapb.ReadConsistency_READ_CONSISTENCY_STRONG,
				ReadPreference:  metadatapb.ReadPreference_READ_PREFERENCE_LEADER_ONLY,
			},
			StoreAddr: store.GetClientAddr(),
			Client:    client,
		}
		p.cacheRoute(resp.GetRegionDescriptor(), route)
		return route, nil
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
	p.invalidateRegionLocked(notLeader.GetRegionId())
	p.mu.Unlock()
}

func (p *CoordinatorRouteProvider) ObserveRouteFailure(_ context.Context, _ []byte, route MetadataRoute, _ error) {
	if p == nil || route.Context == nil || route.Context.GetPeer() == nil {
		return
	}
	regionID := route.Context.GetRegionId()
	peerKey := routePeerFailureKey(route.Context.GetPeer())
	if regionID == 0 || peerKey == "" {
		return
	}
	target := normalizeStoreTarget(route.StoreAddr)
	var staleConn *grpc.ClientConn
	p.mu.Lock()
	if p.failedPeers == nil {
		p.failedPeers = make(map[uint64]map[string]time.Time)
	}
	if p.failedPeers[regionID] == nil {
		p.failedPeers[regionID] = make(map[string]time.Time)
	}
	p.failedPeers[regionID][peerKey] = time.Now().Add(p.dialTimeout)
	p.invalidateRegionLocked(regionID)
	if target != "" {
		if cached, ok := p.clients[target]; ok {
			staleConn = cached.conn
			delete(p.clients, target)
		}
	}
	p.mu.Unlock()
	if staleConn != nil {
		_ = staleConn.Close()
	}
}

func (p *CoordinatorRouteProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	clients := p.clients
	p.clients = make(map[string]storeClient)
	p.routeCache = nil
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

func (p *CoordinatorRouteProvider) cachedRouteForKey(key []byte, now time.Time) (MetadataRoute, bool) {
	if p == nil {
		return MetadataRoute{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cached := range p.routeCache {
		if !cached.containsKey(key) || cached.context == nil || cached.context.GetPeer() == nil {
			continue
		}
		peerKey := routePeerFailureKey(cached.context.GetPeer())
		if p.peerFailureActiveLocked(cached.regionID, peerKey, now) {
			continue
		}
		return cached.toRoute(), true
	}
	return MetadataRoute{}, false
}

func (p *CoordinatorRouteProvider) cacheRoute(desc *metapb.RegionDescriptor, route MetadataRoute) {
	if p == nil || desc == nil || route.Context == nil || route.Client == nil {
		return
	}
	regionID := desc.GetRegionId()
	if regionID == 0 {
		return
	}
	cached := cachedMetadataRoute{
		startKey:  cloneBytes(desc.GetStartKey()),
		endKey:    cloneBytes(desc.GetEndKey()),
		regionID:  regionID,
		context:   cloneMetadataContext(route.Context),
		storeAddr: route.StoreAddr,
		client:    route.Client,
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.invalidateRegionLocked(regionID)
	p.routeCache = append(p.routeCache, cached)
}

func (p *CoordinatorRouteProvider) invalidateRegionLocked(regionID uint64) {
	if regionID == 0 || len(p.routeCache) == 0 {
		return
	}
	out := p.routeCache[:0]
	for _, cached := range p.routeCache {
		if cached.regionID != regionID {
			out = append(out, cached)
		}
	}
	p.routeCache = out
}

func (p *CoordinatorRouteProvider) peerFailureActive(regionID uint64, peer *metapb.RegionPeer, now time.Time) bool {
	key := routePeerFailureKey(peer)
	if p == nil || regionID == 0 || key == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peerFailureActiveLocked(regionID, key, now)
}

func (p *CoordinatorRouteProvider) peerFailureActiveLocked(regionID uint64, peerKey string, now time.Time) bool {
	if p == nil || regionID == 0 || peerKey == "" {
		return false
	}
	failed := p.failedPeers[regionID]
	if len(failed) == 0 {
		return false
	}
	until, ok := failed[peerKey]
	if !ok {
		return false
	}
	if !now.Before(until) {
		delete(failed, peerKey)
		if len(failed) == 0 {
			delete(p.failedPeers, regionID)
		}
		return false
	}
	return true
}

func (r cachedMetadataRoute) containsKey(key []byte) bool {
	if len(r.startKey) != 0 && bytes.Compare(key, r.startKey) < 0 {
		return false
	}
	if len(r.endKey) != 0 && bytes.Compare(key, r.endKey) >= 0 {
		return false
	}
	return true
}

func (r cachedMetadataRoute) toRoute() MetadataRoute {
	return MetadataRoute{
		Context:   cloneMetadataContext(r.context),
		StoreAddr: r.storeAddr,
		Client:    r.client,
	}
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
	add(hint)
	add(leader)
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

func routePeerFailureKey(peer *metapb.RegionPeer) string {
	if peer == nil || peer.GetStoreId() == 0 || peer.GetPeerId() == 0 {
		return ""
	}
	return fmt.Sprintf("%d/%d", peer.GetStoreId(), peer.GetPeerId())
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
