// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	coordprotocol "github.com/feichai0017/NoKV/coordinator/protocol"
	nokverrors "github.com/feichai0017/NoKV/errors"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Client defines the Coordinator control-plane RPC contract consumed by stores.
type Client interface {
	StoreHeartbeat(ctx context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error)
	RegionLiveness(ctx context.Context, req *coordpb.RegionLivenessRequest) (*coordpb.RegionLivenessResponse, error)
	PublishRootEvent(ctx context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error)
	ListTransitions(ctx context.Context, req *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error)
	AssessRootEvent(ctx context.Context, req *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error)
	RemoveRegion(ctx context.Context, req *coordpb.RemoveRegionRequest) (*coordpb.RemoveRegionResponse, error)
	GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error)
	AllocID(ctx context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error)
	Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error)
	Close() error
	GetStore(ctx context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error)
	ListStores(ctx context.Context, req *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error)
	GetMount(ctx context.Context, req *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
	ListMounts(ctx context.Context, req *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
	ListSubtreeAuthorities(ctx context.Context, req *coordpb.ListSubtreeAuthoritiesRequest) (*coordpb.ListSubtreeAuthoritiesResponse, error)
	GetQuotaFence(ctx context.Context, req *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error)
	ListQuotaFences(ctx context.Context, req *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error)
	WatchRootEvents(ctx context.Context, req *coordpb.WatchRootEventsRequest, opts ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error)
}

// GRPCClient is a thin wrapper around generated coordpb.CoordinatorClient.
type GRPCClient struct {
	mu              sync.Mutex
	endpoints       []grpcEndpoint
	preferred       int
	preferredByDuty map[rootproto.DutyID]int

	verifyMu             sync.Mutex
	verifierStore        AuthorityVerifierStore
	verifierClusterID    string
	authorityClockSkew   time.Duration
	authorityMaxReplyAge time.Duration
	now                  func() time.Time
	allocGen             witnessEraFloor
	tsoGen               witnessEraFloor
	metadataGen          witnessEraFloor
	metadataAttached     metadataAttachedFloor
}

type grpcEndpoint struct {
	addr  string
	conn  *grpc.ClientConn
	coord coordpb.CoordinatorClient
}

const (
	maxAuthorityMissRetryRounds        = 8
	authorityMissRetryBackoff          = 20 * time.Millisecond
	authorityMissRetryMaxBackoff       = 120 * time.Millisecond
	defaultAuthorityClockSkewAllowance = time.Second
	defaultAuthorityMaxReplyAge        = 30 * time.Second
)

type GRPCClientOptions struct {
	VerifierStore               AuthorityVerifierStore
	VerifierClusterID           string
	AuthorityClockSkewAllowance time.Duration
	AuthorityMaxReplyAge        time.Duration
	Now                         func() time.Time
}

// NewGRPCClient dials a Coordinator endpoint and returns a ready client.
func NewGRPCClient(ctx context.Context, addr string, dialOpts ...grpc.DialOption) (*GRPCClient, error) {
	return NewGRPCClientWithOptions(ctx, addr, GRPCClientOptions{}, dialOpts...)
}

func NewGRPCClientWithOptions(ctx context.Context, addr string, clientOpts GRPCClientOptions, dialOpts ...grpc.DialOption) (*GRPCClient, error) {
	addrs, err := splitAddresses(addr)
	if err != nil {
		return nil, err
	}
	opts := normalizeDialOptions(dialOpts)
	endpoints := make([]grpcEndpoint, 0, len(addrs))
	for _, target := range addrs {
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			closeAllEndpoints(endpoints)
			return nil, err
		}
		if err := waitForReady(ctx, conn); err != nil {
			_ = conn.Close()
			closeAllEndpoints(endpoints)
			return nil, err
		}
		endpoints = append(endpoints, grpcEndpoint{
			addr:  target,
			conn:  conn,
			coord: coordpb.NewCoordinatorClient(conn),
		})
	}
	store := clientOpts.VerifierStore
	if store == nil {
		store = NewMemoryAuthorityVerifierStore()
	}
	clusterID := strings.TrimSpace(clientOpts.VerifierClusterID)
	if clusterID == "" {
		clusterID = "default"
	}
	clockSkew := clientOpts.AuthorityClockSkewAllowance
	if clockSkew <= 0 {
		clockSkew = defaultAuthorityClockSkewAllowance
	}
	maxReplyAge := clientOpts.AuthorityMaxReplyAge
	if maxReplyAge <= 0 {
		maxReplyAge = defaultAuthorityMaxReplyAge
	}
	nowFn := clientOpts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &GRPCClient{
		endpoints:            endpoints,
		preferredByDuty:      make(map[rootproto.DutyID]int),
		verifierStore:        store,
		verifierClusterID:    clusterID,
		authorityClockSkew:   clockSkew,
		authorityMaxReplyAge: maxReplyAge,
		now:                  nowFn,
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for i := range c.endpoints {
		conn := c.endpoints[i].conn
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.endpoints[i].conn = nil
	}
	return firstErr
}

// StoreHeartbeat fans one heartbeat out to every known coordinator endpoint.
// The control plane treats store stats and region-leader claims as runtime
// telemetry rather than rooted truth, so all coordinators need to observe
// them directly. If any endpoint produces scheduling operations, that response
// wins; otherwise the first successful response is returned.
func (c *GRPCClient) StoreHeartbeat(ctx context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	if c == nil {
		return nil, errNoReachableAddress
	}
	endpoints := c.orderedEndpoints()
	if len(endpoints) == 0 {
		return nil, errNoReachableAddress
	}

	var (
		firstResp     *coordpb.StoreHeartbeatResponse
		firstRespAddr string
		opsResp       *coordpb.StoreHeartbeatResponse
		opsRespAddr   string
		lastErr       error
	)
	for _, endpoint := range endpoints {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				lastErr = err
				break
			}
		}
		resp, err := endpoint.coord.StoreHeartbeat(ctx, req)
		if err != nil {
			lastErr = err
			continue
		}
		if firstResp == nil {
			firstResp = resp
			firstRespAddr = endpoint.addr
		}
		if opsResp == nil && len(resp.GetOperations()) > 0 {
			opsResp = resp
			opsRespAddr = endpoint.addr
		}
	}
	if opsResp != nil {
		c.markPreferred(opsRespAddr)
		return opsResp, nil
	}
	if firstResp != nil {
		c.markPreferred(firstRespAddr)
		return firstResp, nil
	}
	if lastErr == nil {
		lastErr = errNoReachableAddress
	}
	return nil, lastErr
}

// RegionLiveness forwards region liveness heartbeat RPC.
func (c *GRPCClient) RegionLiveness(ctx context.Context, req *coordpb.RegionLivenessRequest) (*coordpb.RegionLivenessResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.RegionLivenessResponse, error) {
		return coord.RegionLiveness(ctx, req)
	}, nil)
}

// PublishRootEvent forwards explicit rooted event RPC.
func (c *GRPCClient) PublishRootEvent(ctx context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	return invokeRPCValidated(ctx, c, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.PublishRootEventResponse, error) {
		return coord.PublishRootEvent(ctx, req)
	}, nil)
}

// ListTransitions returns the rooted pending transition view.
func (c *GRPCClient) ListTransitions(ctx context.Context, req *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.ListTransitionsResponse, error) {
		return coord.ListTransitions(ctx, req)
	}, nil)
}

// AssessRootEvent evaluates one rooted transition event without mutating truth.
func (c *GRPCClient) AssessRootEvent(ctx context.Context, req *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.AssessRootEventResponse, error) {
		return coord.AssessRootEvent(ctx, req)
	}, nil)
}

// RemoveRegion forwards region removal RPC.
func (c *GRPCClient) RemoveRegion(ctx context.Context, req *coordpb.RemoveRegionRequest) (*coordpb.RemoveRegionResponse, error) {
	return invokeRPCValidated(ctx, c, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.RemoveRegionResponse, error) {
		return coord.RemoveRegion(ctx, req)
	}, nil)
}

// GetRegionByKey forwards region lookup RPC.
func (c *GRPCClient) GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	// Region lookup is a metadata authority read: standby coordinators can
	// reject it with grant-not-held, so it must fail over like TSO/AllocID even
	// though the RPC does not mutate user metadata.
	return invokeDutyRPCValidated(ctx, c, rootproto.DutyRegionLookup, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.GetRegionByKeyResponse, error) {
		return coord.GetRegionByKey(ctx, req)
	}, func(resp *coordpb.GetRegionByKeyResponse) error {
		return c.validateGetRegionByKeyResponse(req, resp)
	})
}

// GetStore returns the current runtime endpoint for one store.
func (c *GRPCClient) GetStore(ctx context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.GetStoreResponse, error) {
		return coord.GetStore(ctx, req)
	}, validateGetStoreResponse)
}

// ListStores returns the current runtime store registry snapshot.
func (c *GRPCClient) ListStores(ctx context.Context, req *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.ListStoresResponse, error) {
		return coord.ListStores(ctx, req)
	}, validateListStoresResponse)
}

func (c *GRPCClient) GetMount(ctx context.Context, req *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.GetMountResponse, error) {
		return coord.GetMount(ctx, req)
	}, validateGetMountResponse)
}

func (c *GRPCClient) ListMounts(ctx context.Context, req *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.ListMountsResponse, error) {
		return coord.ListMounts(ctx, req)
	}, validateListMountsResponse)
}

func (c *GRPCClient) ListSubtreeAuthorities(ctx context.Context, req *coordpb.ListSubtreeAuthoritiesRequest) (*coordpb.ListSubtreeAuthoritiesResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.ListSubtreeAuthoritiesResponse, error) {
		return coord.ListSubtreeAuthorities(ctx, req)
	}, validateListSubtreeAuthoritiesResponse)
}

func (c *GRPCClient) GetQuotaFence(ctx context.Context, req *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.GetQuotaFenceResponse, error) {
		return coord.GetQuotaFence(ctx, req)
	}, validateGetQuotaFenceResponse)
}

func (c *GRPCClient) ListQuotaFences(ctx context.Context, req *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error) {
	return invokeRPCValidated(ctx, c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.ListQuotaFencesResponse, error) {
		return coord.ListQuotaFences(ctx, req)
	}, validateListQuotaFencesResponse)
}

func (c *GRPCClient) WatchRootEvents(ctx context.Context, req *coordpb.WatchRootEventsRequest, opts ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
	if c == nil {
		return nil, errNoReachableAddress
	}
	endpoints := c.orderedEndpoints()
	if len(endpoints) == 0 {
		return nil, errNoReachableAddress
	}
	var lastErr error
	for i, endpoint := range endpoints {
		stream, err := endpoint.coord.WatchRootEvents(ctx, req, opts...)
		if err == nil {
			c.markPreferred(endpoint.addr)
			return stream, nil
		}
		lastErr = err
		if i == len(endpoints)-1 || !retryableRead(err) {
			return nil, err
		}
	}
	if lastErr == nil {
		lastErr = errNoReachableAddress
	}
	return nil, lastErr
}

// AllocID forwards ID allocation RPC.
func (c *GRPCClient) AllocID(ctx context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	return invokeDutyRPCValidated(ctx, c, rootproto.DutyAllocID, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.AllocIDResponse, error) {
		return coord.AllocID(ctx, req)
	}, func(resp *coordpb.AllocIDResponse) error {
		return c.validateAllocIDResponse(req, resp)
	})
}

// Tso forwards TSO allocation RPC.
func (c *GRPCClient) Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	return invokeDutyRPCValidated(ctx, c, rootproto.DutyTSO, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.TsoResponse, error) {
		return coord.Tso(ctx, req)
	}, func(resp *coordpb.TsoResponse) error {
		return c.validateTSOResponse(req, resp)
	})
}

func normalizeDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	if len(opts) == 0 {
		return []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				MinConnectTimeout: 2 * time.Second,
			}),
		}
	}
	return opts
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	if ctx == nil {
		return nil
	}
	conn.Connect()
	for {
		state := conn.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return errConnectionShutdown
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func splitAddresses(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errEmptyAddress
	}
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		addrs = append(addrs, part)
	}
	if len(addrs) == 0 {
		return nil, errEmptyAddress
	}
	return addrs, nil
}

func closeAllEndpoints(endpoints []grpcEndpoint) {
	for _, endpoint := range endpoints {
		if endpoint.conn != nil {
			_ = endpoint.conn.Close()
		}
	}
}

func (c *GRPCClient) orderedEndpoints() []grpcEndpoint {
	return c.orderedEndpointsForDuty("")
}

func (c *GRPCClient) orderedEndpointsForDuty(duty rootproto.DutyID) []grpcEndpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.endpoints) == 0 {
		return nil
	}
	out := make([]grpcEndpoint, 0, len(c.endpoints))
	start := c.preferred
	if duty != "" {
		if preferred, ok := c.preferredByDuty[duty]; ok {
			start = preferred
		}
	}
	if start < 0 || start >= len(c.endpoints) {
		start = 0
	}
	for i := 0; i < len(c.endpoints); i++ {
		out = append(out, c.endpoints[(start+i)%len(c.endpoints)])
	}
	return out
}

func (c *GRPCClient) markPreferred(addr string) {
	c.markPreferredForDuty("", addr)
}

func (c *GRPCClient) markPreferredForDuty(duty rootproto.DutyID, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, endpoint := range c.endpoints {
		if endpoint.addr == addr {
			if duty == "" {
				c.preferred = i
			} else {
				if c.preferredByDuty == nil {
					c.preferredByDuty = make(map[rootproto.DutyID]int)
				}
				c.preferredByDuty[duty] = i
			}
			return
		}
	}
}

func invokeRPCValidated[T any](ctx context.Context, c *GRPCClient, retryable func(error) bool, call func(coord coordpb.CoordinatorClient) (T, error), validate func(T) error) (T, error) {
	return invokeDutyRPCValidated(ctx, c, "", retryable, call, validate)
}

func invokeDutyRPCValidated[T any](ctx context.Context, c *GRPCClient, duty rootproto.DutyID, retryable func(error) bool, call func(coord coordpb.CoordinatorClient) (T, error), validate func(T) error) (T, error) {
	var zero T
	if c == nil {
		return zero, errNoReachableAddress
	}
	var lastErr error
	for round := range maxAuthorityMissRetryRounds {
		endpoints := c.orderedEndpointsForDuty(duty)
		if len(endpoints) == 0 {
			return zero, errNoReachableAddress
		}
		// Not-leader / grant-not-held replies mean the client reached a coordinator,
		// but not the current authority. A holder can also be briefly unavailable
		// during root grant renewal or allocator persistence. In both cases retry
		// the whole endpoint set as a unit so a standby rejection from the last
		// endpoint does not mask a converging holder.
		allAuthorityMiss := true
		allRetryable := true
		roundConverging := true
		var lastNonAuthorityErr error
		for i, endpoint := range endpoints {
			resp, err := call(endpoint.coord)
			if err == nil && validate != nil {
				err = validate(resp)
			}
			if err == nil {
				c.markPreferredForDuty(duty, endpoint.addr)
				return resp, nil
			}
			lastErr = err
			if !isAuthorityMiss(err) {
				allAuthorityMiss = false
				lastNonAuthorityErr = err
			}
			if !isDutyConvergenceError(err) {
				roundConverging = false
			}
			if !retryable(err) {
				return zero, err
			}
			if i == len(endpoints)-1 && roundConverging && round+1 < maxAuthorityMissRetryRounds {
				if waitErr := waitAuthorityMissRetry(ctx, round); waitErr != nil {
					return zero, waitErr
				}
				break
			}
			allRetryable = allRetryable && retryable(err)
		}
		if !allRetryable {
			if lastNonAuthorityErr != nil {
				return zero, lastNonAuthorityErr
			}
			return zero, lastErr
		}
		if !allAuthorityMiss && (!roundConverging || round+1 >= maxAuthorityMissRetryRounds) {
			if lastNonAuthorityErr != nil {
				return zero, lastNonAuthorityErr
			}
			return zero, lastErr
		}
	}
	if lastErr == nil {
		lastErr = errNoReachableAddress
	}
	return zero, lastErr
}

func isDutyConvergenceError(err error) bool {
	if isAuthorityMiss(err) {
		return true
	}
	switch nokverrors.KindOf(err) {
	case nokverrors.KindUnavailable, nokverrors.KindRetryable, nokverrors.KindStaleEpoch:
		return true
	default:
		return false
	}
}

func waitAuthorityMissRetry(ctx context.Context, round int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// All endpoints missing the same duty usually means root grant issue,
	// renewal, or route-view publication is still converging. Back off within
	// the caller's deadline instead of leaking that short authority gap to the
	// metadata operation.
	backoff := authorityMissRetryBackoff
	for range round {
		if backoff >= authorityMissRetryMaxBackoff/2 {
			backoff = authorityMissRetryMaxBackoff
			break
		}
		backoff *= 2
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryableRead(err error) bool {
	switch nokverrors.KindOf(err) {
	case nokverrors.KindUnavailable,
		nokverrors.KindRouteUnavailable,
		nokverrors.KindRegionRouting,
		nokverrors.KindStaleEpoch,
		nokverrors.KindRetryable:
		return true
	default:
		return false
	}
}

func retryableWrite(err error) bool {
	if retryableRead(err) {
		return true
	}
	return nokverrors.IsKind(err, nokverrors.KindNotLeader)
}

func isAuthorityMiss(err error) bool {
	return nokverrors.IsKind(err, nokverrors.KindNotLeader)
}

type witnessEraFloor struct {
	maxSeen     uint64
	retiredSeen uint64
}

type metadataAttachedFloor struct {
	hasCurrentToken bool
	currentToken    *coordpb.RootToken
}

type metadataWitnessExpectation struct {
	freshness                  coordpb.Freshness
	requiredRootToken          *coordpb.RootToken
	requiredDescriptorRevision uint64
	maxRootLag                 *uint64
}

func (c *GRPCClient) validateAllocIDResponse(req *coordpb.AllocIDRequest, resp *coordpb.AllocIDResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: alloc id response is nil", errInvalidWitness)
	}
	return c.validateMonotoneWitness("alloc_id", rootproto.DutyAllocID, normalizedCount(requestedAllocIDCount(req)), resp.GetFirstId(), resp.GetCount(), resp.GetEra(), resp.GetConsumedFrontier(), resp.GetObservedRetiredEraFloor(), resp.GetAuthorityEvidence(), &c.allocGen)
}

func (c *GRPCClient) validateGetRegionByKeyResponse(req *coordpb.GetRegionByKeyRequest, resp *coordpb.GetRegionByKeyResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: get_region_by_key response is nil", errInvalidWitness)
	}
	expectation := newMetadataWitnessExpectation(req)
	if resp.GetServedFreshness() != expectation.freshness {
		return fmt.Errorf("%w: served_freshness=%s requested=%s", errInvalidWitness, resp.GetServedFreshness(), expectation.freshness)
	}
	if resp.GetRequiredDescriptorRevision() != expectation.requiredDescriptorRevision {
		return fmt.Errorf("%w: required_descriptor_revision=%d requested=%d", errInvalidWitness, resp.GetRequiredDescriptorRevision(), expectation.requiredDescriptorRevision)
	}
	if !metadataRootTokenSatisfied(resp.GetServedRootToken(), expectation.requiredRootToken) {
		return fmt.Errorf("%w: served_root_token does not satisfy required_root_token", errInvalidWitness)
	}
	if !metadataRootTokenSatisfied(resp.GetCurrentRootToken(), resp.GetServedRootToken()) {
		return fmt.Errorf("%w: current_root_token regressed behind served_root_token", errInvalidWitness)
	}
	expectedRootLag := metadataRootLag(resp.GetCurrentRootToken(), resp.GetServedRootToken())
	if resp.GetRootLag() != expectedRootLag {
		return fmt.Errorf("%w: root_lag=%d expected=%d", errInvalidWitness, resp.GetRootLag(), expectedRootLag)
	}
	if expectation.freshness == coordpb.Freshness_FRESHNESS_BOUNDED && expectation.maxRootLag != nil && resp.GetRootLag() > *expectation.maxRootLag {
		return fmt.Errorf("%w: root_lag=%d exceeds max_root_lag=%d", errInvalidWitness, resp.GetRootLag(), *expectation.maxRootLag)
	}
	expectedServingClass, expectedSyncHealth := coordprotocol.MetadataServingContract(
		resp.GetDegradedMode(),
		resp.GetCatchUpState(),
		resp.GetRootLag(),
		resp.GetServedByLeader(),
	)
	if resp.GetServingClass() != expectedServingClass {
		return fmt.Errorf("%w: serving_class=%s expected=%s", errInvalidWitness, resp.GetServingClass(), expectedServingClass)
	}
	if resp.GetSyncHealth() != expectedSyncHealth {
		return fmt.Errorf("%w: sync_health=%s expected=%s", errInvalidWitness, resp.GetSyncHealth(), expectedSyncHealth)
	}
	if err := metadataFreshnessSatisfied(expectation.freshness, resp); err != nil {
		return err
	}
	if err := c.validateMetadataWitnessEra(resp); err != nil {
		return err
	}
	if resp.GetNotFound() {
		if resp.GetRegionDescriptor() != nil {
			return fmt.Errorf("%w: not_found reply carries region descriptor", errInvalidWitness)
		}
		return nil
	}
	if resp.GetRegionDescriptor() == nil {
		return fmt.Errorf("%w: missing region descriptor on non-not-found reply", errInvalidWitness)
	}
	if resp.GetDescriptorRevision() != resp.GetRegionDescriptor().GetRootEpoch() {
		return fmt.Errorf("%w: descriptor_revision=%d topology.root_epoch=%d", errInvalidWitness, resp.GetDescriptorRevision(), resp.GetRegionDescriptor().GetRootEpoch())
	}
	if resp.GetDescriptorRevision() < expectation.requiredDescriptorRevision {
		return fmt.Errorf("%w: descriptor_revision=%d required=%d", errInvalidWitness, resp.GetDescriptorRevision(), expectation.requiredDescriptorRevision)
	}
	if leader := resp.GetLeaderPeer(); leader != nil && !descriptorHasPeer(resp.GetRegionDescriptor(), leader) {
		return fmt.Errorf("%w: leader_peer store=%d peer=%d is not in descriptor", errInvalidWitness, leader.GetStoreId(), leader.GetPeerId())
	}
	return nil
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

func validateGetStoreResponse(resp *coordpb.GetStoreResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: get_store response is nil", errInvalidWitness)
	}
	if resp.GetNotFound() {
		if resp.GetStore() != nil {
			return fmt.Errorf("%w: get_store not_found reply carries store", errInvalidWitness)
		}
		return nil
	}
	store := resp.GetStore()
	if store == nil {
		return fmt.Errorf("%w: get_store missing store on non-not-found reply", errInvalidWitness)
	}
	if store.GetStoreId() == 0 {
		return fmt.Errorf("%w: get_store store_id is zero", errInvalidWitness)
	}
	if store.GetClientAddr() == "" && store.GetState() == coordpb.StoreState_STORE_STATE_UP {
		return fmt.Errorf("%w: get_store client_addr is empty", errInvalidWitness)
	}
	return nil
}

func validateListStoresResponse(resp *coordpb.ListStoresResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: list_stores response is nil", errInvalidWitness)
	}
	seen := make(map[uint64]struct{}, len(resp.GetStores()))
	for _, store := range resp.GetStores() {
		if store == nil {
			return fmt.Errorf("%w: list_stores contains nil store", errInvalidWitness)
		}
		if store.GetStoreId() == 0 {
			return fmt.Errorf("%w: list_stores contains zero store_id", errInvalidWitness)
		}
		if store.GetClientAddr() == "" && store.GetState() == coordpb.StoreState_STORE_STATE_UP {
			return fmt.Errorf("%w: list_stores store %d has empty client_addr", errInvalidWitness, store.GetStoreId())
		}
		if _, ok := seen[store.GetStoreId()]; ok {
			return fmt.Errorf("%w: list_stores duplicate store_id=%d", errInvalidWitness, store.GetStoreId())
		}
		seen[store.GetStoreId()] = struct{}{}
	}
	return nil
}

func validateGetMountResponse(resp *coordpb.GetMountResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: get_mount response is nil", errInvalidWitness)
	}
	if resp.GetNotFound() {
		if resp.GetMount() != nil {
			return fmt.Errorf("%w: get_mount not_found reply carries mount", errInvalidWitness)
		}
		return nil
	}
	mount := resp.GetMount()
	if mount == nil {
		return fmt.Errorf("%w: get_mount missing mount on non-not-found reply", errInvalidWitness)
	}
	if mount.GetMountId() == "" {
		return fmt.Errorf("%w: get_mount mount_id is empty", errInvalidWitness)
	}
	if mount.GetMountKeyId() == 0 {
		return fmt.Errorf("%w: get_mount mount_key_id is empty", errInvalidWitness)
	}
	return nil
}

func validateListMountsResponse(resp *coordpb.ListMountsResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: list_mounts response is nil", errInvalidWitness)
	}
	seen := make(map[string]struct{}, len(resp.GetMounts()))
	seenKeyID := make(map[uint64]struct{}, len(resp.GetMounts()))
	for _, mount := range resp.GetMounts() {
		if mount == nil {
			return fmt.Errorf("%w: list_mounts contains nil mount", errInvalidWitness)
		}
		if mount.GetMountId() == "" {
			return fmt.Errorf("%w: list_mounts contains empty mount_id", errInvalidWitness)
		}
		if mount.GetMountKeyId() == 0 {
			return fmt.Errorf("%w: list_mounts contains empty mount_key_id", errInvalidWitness)
		}
		if _, ok := seen[mount.GetMountId()]; ok {
			return fmt.Errorf("%w: list_mounts duplicate mount_id=%s", errInvalidWitness, mount.GetMountId())
		}
		seen[mount.GetMountId()] = struct{}{}
		if _, ok := seenKeyID[mount.GetMountKeyId()]; ok {
			return fmt.Errorf("%w: list_mounts duplicate mount_key_id=%d", errInvalidWitness, mount.GetMountKeyId())
		}
		seenKeyID[mount.GetMountKeyId()] = struct{}{}
	}
	return nil
}

func validateListSubtreeAuthoritiesResponse(resp *coordpb.ListSubtreeAuthoritiesResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: list_subtree_authorities response is nil", errInvalidWitness)
	}
	seen := make(map[string]struct{}, len(resp.GetSubtrees()))
	for _, subtree := range resp.GetSubtrees() {
		if subtree == nil {
			return fmt.Errorf("%w: list_subtree_authorities contains nil subtree", errInvalidWitness)
		}
		if subtree.GetSubtreeId() == "" || subtree.GetMountId() == "" || subtree.GetRootInode() == 0 {
			return fmt.Errorf("%w: list_subtree_authorities contains invalid subtree", errInvalidWitness)
		}
		if _, ok := seen[subtree.GetSubtreeId()]; ok {
			return fmt.Errorf("%w: list_subtree_authorities duplicate subtree=%s", errInvalidWitness, subtree.GetSubtreeId())
		}
		seen[subtree.GetSubtreeId()] = struct{}{}
	}
	return nil
}

func validateGetQuotaFenceResponse(resp *coordpb.GetQuotaFenceResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: get_quota_fence response is nil", errInvalidWitness)
	}
	if resp.GetNotFound() {
		if resp.GetFence() != nil {
			return fmt.Errorf("%w: get_quota_fence not_found reply carries fence", errInvalidWitness)
		}
		return nil
	}
	return validateQuotaFenceInfo("get_quota_fence", resp.GetFence())
}

func validateListQuotaFencesResponse(resp *coordpb.ListQuotaFencesResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: list_quota_fences response is nil", errInvalidWitness)
	}
	seen := make(map[string]struct{}, len(resp.GetFences()))
	for _, fence := range resp.GetFences() {
		if err := validateQuotaFenceInfo("list_quota_fences", fence); err != nil {
			return err
		}
		key := fmt.Sprintf("%s/%d", fence.GetSubject().GetMountId(), fence.GetSubject().GetSubtreeRoot())
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%w: list_quota_fences duplicate subject=%s", errInvalidWitness, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateQuotaFenceInfo(kind string, fence *coordpb.QuotaFenceInfo) error {
	if fence == nil {
		return fmt.Errorf("%w: %s missing fence on non-not-found reply", errInvalidWitness, kind)
	}
	subject := fence.GetSubject()
	if subject == nil || subject.GetMountId() == "" {
		return fmt.Errorf("%w: %s missing quota subject", errInvalidWitness, kind)
	}
	return nil
}

func (c *GRPCClient) validateTSOResponse(req *coordpb.TsoRequest, resp *coordpb.TsoResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: tso response is nil", errInvalidWitness)
	}
	return c.validateMonotoneWitness("tso", rootproto.DutyTSO, normalizedCount(requestedTSOCount(req)), resp.GetTimestamp(), resp.GetCount(), resp.GetEra(), resp.GetConsumedFrontier(), resp.GetObservedRetiredEraFloor(), resp.GetAuthorityEvidence(), &c.tsoGen)
}

func (c *GRPCClient) validateMonotoneWitness(kind string, duty rootproto.DutyID, requestedCount, first, gotCount, era, consumedFrontier, observedRetiredEraFloor uint64, evidence *metapb.RootAuthorityEvidence, floor *witnessEraFloor) error {
	if era == rootproto.AuthorityEraSuppressed {
		return fmt.Errorf("%w: %s reply evidence suppressed", errInvalidWitness, kind)
	}
	if gotCount != requestedCount {
		return fmt.Errorf("%w: %s count=%d requested=%d", errInvalidWitness, kind, gotCount, requestedCount)
	}
	expectedFrontier, err := expectedConsumedFrontier(first, gotCount)
	if err != nil {
		return fmt.Errorf("%w: %s %v", errInvalidWitness, kind, err)
	}
	if consumedFrontier != expectedFrontier {
		return fmt.Errorf("%w: %s consumed_frontier=%d expected=%d", errInvalidWitness, kind, consumedFrontier, expectedFrontier)
	}
	if err := c.validateAuthorityEvidence(kind, duty, era, observedRetiredEraFloor, rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedFrontier}, evidence); err != nil {
		return err
	}
	return c.advanceWitnessEraFloor(kind, duty, era, observedRetiredEraFloor, floor)
}

func (c *GRPCClient) validateMetadataWitnessEra(resp *coordpb.GetRegionByKeyResponse) error {
	era := resp.GetEra()
	if era == rootproto.AuthorityEraSuppressed {
		return fmt.Errorf("%w: get_region_by_key reply evidence suppressed", errInvalidWitness)
	}
	if era == rootproto.AuthorityEraAttached {
		if resp.GetServingClass() != coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE ||
			resp.GetSyncHealth() != coordpb.SyncHealth_SYNC_HEALTH_HEALTHY ||
			!resp.GetServedByLeader() ||
			resp.GetRootLag() != 0 ||
			resp.GetCatchUpState() != coordpb.CatchUpState_CATCH_UP_STATE_FRESH {
			return fmt.Errorf(
				"%w: get_region_by_key era=0 requires authoritative attached serving_class=%s sync_health=%s served_by_leader=%t root_lag=%d catch_up_state=%s",
				errInvalidWitness,
				resp.GetServingClass(),
				resp.GetSyncHealth(),
				resp.GetServedByLeader(),
				resp.GetRootLag(),
				resp.GetCatchUpState(),
			)
		}
		return c.advanceAttachedMetadataFloor(resp)
	}
	if err := c.validateAuthorityEvidence("get_region_by_key", rootproto.DutyRegionLookup, era, resp.GetObservedRetiredEraFloor(), rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: resp.GetDescriptorRevision()}, resp.GetAuthorityEvidence()); err != nil {
		return err
	}
	if err := c.advanceWitnessEraFloor("get_region_by_key", rootproto.DutyRegionLookup, era, resp.GetObservedRetiredEraFloor(), &c.metadataGen); err != nil {
		return err
	}
	return c.advanceMetadataVerifierRootFloor(resp)
}

func (c *GRPCClient) validateAuthorityEvidence(kind string, duty rootproto.DutyID, era, observedRetiredEraFloor uint64, required rootproto.DutyBound, pbEvidence *metapb.RootAuthorityEvidence) error {
	if era == rootproto.AuthorityEraAttached {
		if duty != rootproto.DutyRegionLookup {
			return fmt.Errorf("%w: %s attached era is only valid for metadata witnesses", errInvalidWitness, kind)
		}
		if pbEvidence != nil {
			return fmt.Errorf("%w: %s attached reply carries authority evidence", errInvalidWitness, kind)
		}
		return nil
	}
	evidence := metawire.RootAuthorityEvidenceFromProto(pbEvidence)
	cert := evidence.Certificate
	if !cert.Grant.Present() {
		return fmt.Errorf("%w: %s authority evidence missing grant certificate", errInvalidWitness, kind)
	}
	if cert.SignerKeyID != rootproto.GrantSignerKeyID {
		return fmt.Errorf("%w: %s authority evidence signer=%s", errInvalidWitness, kind, cert.SignerKeyID)
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(cert.Grant))
	if err != nil {
		return fmt.Errorf("%w: %s authority evidence marshal: %v", errInvalidWitness, kind, err)
	}
	if !rootproto.VerifyGrantBytes(payload, cert.Signature) {
		return fmt.Errorf("%w: %s authority evidence signature mismatch", errInvalidWitness, kind)
	}
	if cert.Grant.Era != era {
		return fmt.Errorf("%w: %s grant_era=%d reply_era=%d", errInvalidWitness, kind, cert.Grant.Era, era)
	}
	nowUnixNano := time.Now().UnixNano()
	clockSkew := defaultAuthorityClockSkewAllowance
	maxReplyAge := defaultAuthorityMaxReplyAge
	if c != nil {
		if c.now != nil {
			nowUnixNano = c.now().UnixNano()
		}
		if c.authorityClockSkew > 0 {
			clockSkew = c.authorityClockSkew
		}
		if c.authorityMaxReplyAge > 0 {
			maxReplyAge = c.authorityMaxReplyAge
		}
	}
	if evidence.ServedUnixNano <= 0 {
		return fmt.Errorf("%w: %s authority evidence missing served_unix_nano", errInvalidWitness, kind)
	}
	if evidence.ServedUnixNano > nowUnixNano+clockSkew.Nanoseconds() {
		return fmt.Errorf("%w: %s authority evidence served in the future", errInvalidWitness, kind)
	}
	if nowUnixNano-evidence.ServedUnixNano > maxReplyAge.Nanoseconds() {
		return fmt.Errorf("%w: %s authority evidence exceeds max reply age", errInvalidWitness, kind)
	}
	if evidence.ServedUnixNano > cert.Grant.ExpiresUnixNano {
		return fmt.Errorf("%w: %s authority evidence served after grant expiry", errInvalidWitness, kind)
	}
	if cert.Grant.ExpiresUnixNano+clockSkew.Nanoseconds() <= nowUnixNano {
		return fmt.Errorf("%w: %s authority evidence expired", errInvalidWitness, kind)
	}
	if evidence.Usage.DutyID != duty {
		return fmt.Errorf("%w: %s evidence_duty=%s required=%s", errInvalidWitness, kind, evidence.Usage.DutyID, duty)
	}
	if !rootproto.ValidateAuthorityUsage(evidence.Usage) {
		return fmt.Errorf("%w: %s authority evidence has invalid duty scope or usage", errInvalidWitness, kind)
	}
	grantDuty, ok := cert.Grant.DutyFor(duty, evidence.Usage.Scope)
	if !ok ||
		!rootproto.DutyBoundCovers(grantDuty.Bound, evidence.Usage.Usage) ||
		!rootproto.DutyBoundCovers(evidence.Usage.Usage, required) {
		return fmt.Errorf("%w: %s usage outside grant", errInvalidWitness, kind)
	}
	if observedRetiredEraFloor != 0 && evidence.ObservedRetiredEraFloor < observedRetiredEraFloor {
		return fmt.Errorf("%w: %s observed_retired_floor=%d reply_observed=%d", errInvalidWitness, kind, evidence.ObservedRetiredEraFloor, observedRetiredEraFloor)
	}
	return nil
}

func (c *GRPCClient) advanceAttachedMetadataFloor(resp *coordpb.GetRegionByKeyResponse) error {
	c.verifyMu.Lock()
	defer c.verifyMu.Unlock()

	currentToken := resp.GetCurrentRootToken()
	if c.metadataAttached.hasCurrentToken && !metadataRootTokenSatisfied(currentToken, c.metadataAttached.currentToken) {
		return fmt.Errorf(
			"%w: get_region_by_key era=0 current_root_token regressed behind attached floor",
			errInvalidWitness,
		)
	}
	storeState, err := c.loadVerifierStateLocked(rootproto.DutyRegionLookup)
	if err != nil {
		return err
	}
	if !authorityRootTokenZero(storeState.MaxRootToken) &&
		!metadataRootTokenSatisfied(currentToken, authorityRootTokenToCoordProto(storeState.MaxRootToken)) {
		return fmt.Errorf(
			"%w: get_region_by_key era=0 current_root_token regressed behind durable verifier floor",
			errInvalidWitness,
		)
	}
	if currentToken != nil {
		c.metadataAttached.currentToken = proto.Clone(currentToken).(*coordpb.RootToken)
		c.metadataAttached.hasCurrentToken = true
		storeState.MaxRootToken = authorityRootTokenFromCoordProto(currentToken)
	}
	if err := c.saveVerifierStateLocked(storeState); err != nil {
		return err
	}
	return nil
}

func (c *GRPCClient) advanceMetadataVerifierRootFloor(resp *coordpb.GetRegionByKeyResponse) error {
	c.verifyMu.Lock()
	defer c.verifyMu.Unlock()
	storeState, err := c.loadVerifierStateLocked(rootproto.DutyRegionLookup)
	if err != nil {
		return err
	}
	currentToken := resp.GetCurrentRootToken()
	if !authorityRootTokenZero(storeState.MaxRootToken) &&
		!metadataRootTokenSatisfied(currentToken, authorityRootTokenToCoordProto(storeState.MaxRootToken)) {
		return fmt.Errorf("%w: get_region_by_key current_root_token regressed behind durable verifier floor", errInvalidWitness)
	}
	if currentToken != nil {
		storeState.MaxRootToken = authorityRootTokenFromCoordProto(currentToken)
	}
	// DescriptorRevision is the returned region descriptor's own root epoch.
	// Different bucket regions can validly carry lower root epochs than a
	// previously observed hot region, so only the root token is a global durable
	// floor. Per-request descriptor_revision checks stay in
	// validateGetRegionByKeyResponse.
	return c.saveVerifierStateLocked(storeState)
}

func (c *GRPCClient) advanceWitnessEraFloor(kind string, duty rootproto.DutyID, era, observedRetiredEraFloor uint64, floor *witnessEraFloor) error {
	c.verifyMu.Lock()
	defer c.verifyMu.Unlock()
	if era == rootproto.AuthorityEraSuppressed {
		return fmt.Errorf("%w: %s era suppressed", errInvalidWitness, kind)
	}
	storeState, err := c.loadVerifierStateLocked(duty)
	if err != nil {
		return err
	}
	currentRetiredSeen := max(floor.retiredSeen, storeState.RetiredEraFloor)
	currentMaxSeen := max(floor.maxSeen, storeState.MaxSeenEra)
	nextRetiredSeen := max(observedRetiredEraFloor, currentRetiredSeen)
	if nextRetiredSeen != 0 && era <= nextRetiredSeen {
		return fmt.Errorf("%w: %s era=%d retired_floor=%d", errStaleWitnessEra, kind, era, nextRetiredSeen)
	}
	if era < currentMaxSeen {
		return fmt.Errorf("%w: %s era=%d max_seen=%d", errStaleWitnessEra, kind, era, currentMaxSeen)
	}
	nextMaxSeen := max(era, currentMaxSeen)
	storeState.MaxSeenEra = nextMaxSeen
	storeState.RetiredEraFloor = nextRetiredSeen
	if err := c.saveVerifierStateLocked(storeState); err != nil {
		return err
	}
	floor.retiredSeen = nextRetiredSeen
	floor.maxSeen = nextMaxSeen
	return nil
}

func (c *GRPCClient) loadVerifierStateLocked(duty rootproto.DutyID) (AuthorityVerifierState, error) {
	key := c.authorityVerifierKey(duty)
	if c == nil || c.verifierStore == nil {
		return AuthorityVerifierState{Key: key}, nil
	}
	state, err := c.verifierStore.LoadAuthorityVerifier(key)
	if err != nil {
		return AuthorityVerifierState{}, fmt.Errorf("%w: load authority verifier state: %v", errInvalidWitness, err)
	}
	if state.Key.ClusterID == "" {
		state.Key = key
	}
	return state, nil
}

func (c *GRPCClient) saveVerifierStateLocked(state AuthorityVerifierState) error {
	if c == nil || c.verifierStore == nil {
		return nil
	}
	if err := c.verifierStore.SaveAuthorityVerifier(state); err != nil {
		return fmt.Errorf("%w: save authority verifier state: %v", errInvalidWitness, err)
	}
	return nil
}

func (c *GRPCClient) authorityVerifierKey(duty rootproto.DutyID) AuthorityVerifierKey {
	clusterID := "default"
	if c != nil && strings.TrimSpace(c.verifierClusterID) != "" {
		clusterID = strings.TrimSpace(c.verifierClusterID)
	}
	return AuthorityVerifierKey{
		ClusterID: clusterID,
		DutyID:    duty,
		Scope:     rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
	}
}

func requestedAllocIDCount(req *coordpb.AllocIDRequest) uint64 {
	if req == nil {
		return 0
	}
	return req.GetCount()
}

func requestedTSOCount(req *coordpb.TsoRequest) uint64 {
	if req == nil {
		return 0
	}
	return req.GetCount()
}

func normalizedCount(count uint64) uint64 {
	if count == 0 {
		return 1
	}
	return count
}

func expectedConsumedFrontier(first, count uint64) (uint64, error) {
	if count == 0 {
		return 0, fmt.Errorf("count must be >= 1")
	}
	if count == 1 {
		return first, nil
	}
	frontier := first + count - 1
	if frontier < first {
		return 0, fmt.Errorf("frontier overflow first=%d count=%d", first, count)
	}
	return frontier, nil
}

func normalizeMetadataFreshness(req *coordpb.GetRegionByKeyRequest) coordpb.Freshness {
	if req == nil {
		return coordpb.Freshness_FRESHNESS_BEST_EFFORT
	}
	return coordprotocol.NormalizeFreshness(req.GetFreshness())
}

func newMetadataWitnessExpectation(req *coordpb.GetRegionByKeyRequest) metadataWitnessExpectation {
	if req == nil {
		return metadataWitnessExpectation{
			freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
		}
	}
	return metadataWitnessExpectation{
		freshness:                  normalizeMetadataFreshness(req),
		requiredRootToken:          req.GetRequiredRootToken(),
		requiredDescriptorRevision: req.GetRequiredDescriptorRevision(),
		maxRootLag:                 req.MaxRootLag,
	}
}

func metadataRootTokenSatisfied(current, required *coordpb.RootToken) bool {
	if metadataRootTokenZero(required) {
		return true
	}
	if current == nil {
		return false
	}
	if current.GetRevision() != 0 || required.GetRevision() != 0 {
		return current.GetRevision() >= required.GetRevision() && !metadataCursorAfter(required, current)
	}
	return !metadataCursorAfter(required, current)
}

func metadataRootTokenZero(token *coordpb.RootToken) bool {
	return token == nil || (token.GetTerm() == 0 && token.GetIndex() == 0 && token.GetRevision() == 0)
}

func authorityRootTokenZero(token rootproto.AuthorityRootToken) bool {
	return token.Term == 0 && token.Index == 0 && token.Revision == 0
}

func authorityRootTokenFromCoordProto(token *coordpb.RootToken) rootproto.AuthorityRootToken {
	if token == nil {
		return rootproto.AuthorityRootToken{}
	}
	return rootproto.AuthorityRootToken{
		Term:     token.GetTerm(),
		Index:    token.GetIndex(),
		Revision: token.GetRevision(),
	}
}

func authorityRootTokenToCoordProto(token rootproto.AuthorityRootToken) *coordpb.RootToken {
	return &coordpb.RootToken{
		Term:     token.Term,
		Index:    token.Index,
		Revision: token.Revision,
	}
}

func metadataCursorAfter(a, b *coordpb.RootToken) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return a.GetTerm() != 0 || a.GetIndex() != 0
	}
	if a.GetTerm() != b.GetTerm() {
		return a.GetTerm() > b.GetTerm()
	}
	return a.GetIndex() > b.GetIndex()
}

func metadataRootLag(current, served *coordpb.RootToken) uint64 {
	if current == nil {
		current = &coordpb.RootToken{}
	}
	if served == nil {
		served = &coordpb.RootToken{}
	}
	if current.GetRevision() > 0 || served.GetRevision() > 0 {
		if current.GetRevision() > served.GetRevision() {
			return current.GetRevision() - served.GetRevision()
		}
		if current.GetRevision() == served.GetRevision() && metadataCursorAfter(current, served) {
			return 1
		}
		return 0
	}
	if metadataCursorAfter(current, served) {
		return 1
	}
	return 0
}

func metadataFreshnessSatisfied(freshness coordpb.Freshness, resp *coordpb.GetRegionByKeyResponse) error {
	switch freshness {
	case coordpb.Freshness_FRESHNESS_STRONG:
		if resp.GetServingClass() != coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE {
			return fmt.Errorf("%w: strong freshness served as %s", errInvalidWitness, resp.GetServingClass())
		}
	case coordpb.Freshness_FRESHNESS_BOUNDED:
		if resp.GetServingClass() == coordpb.ServingClass_SERVING_CLASS_DEGRADED {
			return fmt.Errorf("%w: bounded freshness degraded with sync_health=%s", errInvalidWitness, resp.GetSyncHealth())
		}
	}
	return nil
}
