package client

import (
	"context"
	"fmt"
	coordablation "github.com/feichai0017/NoKV/coordinator/ablation"
	coordprotocol "github.com/feichai0017/NoKV/coordinator/protocol"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
}

// GRPCClient is a thin wrapper around generated coordpb.CoordinatorClient.
type GRPCClient struct {
	mu        sync.Mutex
	endpoints []grpcEndpoint
	preferred int

	verifyMu         sync.Mutex
	allocGen         witnessGenerationFloor
	tsoGen           witnessGenerationFloor
	metadataGen      witnessGenerationFloor
	metadataAttached metadataAttachedFloor
	ablation         coordablation.Config
}

type grpcEndpoint struct {
	addr  string
	conn  *grpc.ClientConn
	coord coordpb.CoordinatorClient
}

// NewGRPCClient dials a Coordinator endpoint and returns a ready client.
func NewGRPCClient(ctx context.Context, addr string, dialOpts ...grpc.DialOption) (*GRPCClient, error) {
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
	return &GRPCClient{
		endpoints: endpoints,
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
	for _, endpoint := range c.endpoints {
		if endpoint.conn == nil {
			continue
		}
		if err := endpoint.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ConfigureAblation installs first-cut client-side ablation switches for
// verifier experiments. It should be set once during benchmark/test setup.
func (c *GRPCClient) ConfigureAblation(cfg coordablation.Config) error {
	if c == nil {
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.ablation = cfg
	return nil
}

// StoreHeartbeat forwards store heartbeat RPC.
func (c *GRPCClient) StoreHeartbeat(ctx context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	return invokeRPC(c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.StoreHeartbeatResponse, error) {
		return coord.StoreHeartbeat(ctx, req)
	})
}

// RegionLiveness forwards region liveness heartbeat RPC.
func (c *GRPCClient) RegionLiveness(ctx context.Context, req *coordpb.RegionLivenessRequest) (*coordpb.RegionLivenessResponse, error) {
	return invokeRPC(c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.RegionLivenessResponse, error) {
		return coord.RegionLiveness(ctx, req)
	})
}

// PublishRootEvent forwards explicit rooted event RPC.
func (c *GRPCClient) PublishRootEvent(ctx context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	return invokeRPC(c, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.PublishRootEventResponse, error) {
		return coord.PublishRootEvent(ctx, req)
	})
}

// ListTransitions returns the rooted pending transition view.
func (c *GRPCClient) ListTransitions(ctx context.Context, req *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error) {
	return invokeRPC(c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.ListTransitionsResponse, error) {
		return coord.ListTransitions(ctx, req)
	})
}

// AssessRootEvent evaluates one rooted transition event without mutating truth.
func (c *GRPCClient) AssessRootEvent(ctx context.Context, req *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error) {
	return invokeRPC(c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.AssessRootEventResponse, error) {
		return coord.AssessRootEvent(ctx, req)
	})
}

// RemoveRegion forwards region removal RPC.
func (c *GRPCClient) RemoveRegion(ctx context.Context, req *coordpb.RemoveRegionRequest) (*coordpb.RemoveRegionResponse, error) {
	return invokeRPC(c, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.RemoveRegionResponse, error) {
		return coord.RemoveRegion(ctx, req)
	})
}

// GetRegionByKey forwards region lookup RPC.
func (c *GRPCClient) GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	return invokeRPCValidated(c, retryableRead, func(coord coordpb.CoordinatorClient) (*coordpb.GetRegionByKeyResponse, error) {
		return coord.GetRegionByKey(ctx, req)
	}, func(resp *coordpb.GetRegionByKeyResponse) error {
		return c.validateGetRegionByKeyResponse(req, resp)
	})
}

// AllocID forwards ID allocation RPC.
func (c *GRPCClient) AllocID(ctx context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	return invokeRPCValidated(c, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.AllocIDResponse, error) {
		return coord.AllocID(ctx, req)
	}, func(resp *coordpb.AllocIDResponse) error {
		return c.validateAllocIDResponse(req, resp)
	})
}

// Tso forwards TSO allocation RPC.
func (c *GRPCClient) Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	return invokeRPCValidated(c, retryableWrite, func(coord coordpb.CoordinatorClient) (*coordpb.TsoResponse, error) {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.endpoints) == 0 {
		return nil
	}
	out := make([]grpcEndpoint, 0, len(c.endpoints))
	start := c.preferred
	if start < 0 || start >= len(c.endpoints) {
		start = 0
	}
	for i := 0; i < len(c.endpoints); i++ {
		out = append(out, c.endpoints[(start+i)%len(c.endpoints)])
	}
	return out
}

func (c *GRPCClient) markPreferred(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, endpoint := range c.endpoints {
		if endpoint.addr == addr {
			c.preferred = i
			return
		}
	}
}

func invokeRPC[T any](c *GRPCClient, retryable func(error) bool, call func(coord coordpb.CoordinatorClient) (T, error)) (T, error) {
	return invokeRPCValidated(c, retryable, call, nil)
}

func invokeRPCValidated[T any](c *GRPCClient, retryable func(error) bool, call func(coord coordpb.CoordinatorClient) (T, error), validate func(T) error) (T, error) {
	var zero T
	if c == nil {
		return zero, errNoReachableAddress
	}
	if c.ablation.DisableClientVerify {
		validate = nil
	}
	endpoints := c.orderedEndpoints()
	if len(endpoints) == 0 {
		return zero, errNoReachableAddress
	}
	var lastErr error
	for i, endpoint := range endpoints {
		resp, err := call(endpoint.coord)
		if err == nil && validate != nil {
			err = validate(resp)
		}
		if err == nil {
			c.markPreferred(endpoint.addr)
			return resp, nil
		}
		lastErr = err
		if i == len(endpoints)-1 || !retryable(err) {
			return zero, err
		}
	}
	if lastErr == nil {
		lastErr = errNoReachableAddress
	}
	return zero, lastErr
}

func retryableRead(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded || IsStaleWitnessGeneration(err)
}

func retryableWrite(err error) bool {
	if retryableRead(err) {
		return true
	}
	return IsNotLeader(err) || IsLeaseNotHeld(err)
}

type witnessGenerationFloor struct {
	maxSeen    uint64
	sealedSeen uint64
}

type metadataAttachedFloor struct {
	hasCurrentToken    bool
	currentToken       *coordpb.RootToken
	descriptorRevision uint64
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
	return c.validateMonotoneWitness("alloc_id", normalizedCount(requestedAllocIDCount(req)), resp.GetFirstId(), resp.GetCount(), resp.GetCertGeneration(), resp.GetConsumedFrontier(), resp.GetObservedSealGeneration(), &c.allocGen)
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
	if err := c.validateMetadataWitnessGeneration(resp); err != nil {
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
		return fmt.Errorf("%w: descriptor_revision=%d descriptor.root_epoch=%d", errInvalidWitness, resp.GetDescriptorRevision(), resp.GetRegionDescriptor().GetRootEpoch())
	}
	if resp.GetDescriptorRevision() < expectation.requiredDescriptorRevision {
		return fmt.Errorf("%w: descriptor_revision=%d required=%d", errInvalidWitness, resp.GetDescriptorRevision(), expectation.requiredDescriptorRevision)
	}
	return nil
}

func (c *GRPCClient) validateTSOResponse(req *coordpb.TsoRequest, resp *coordpb.TsoResponse) error {
	if resp == nil {
		return fmt.Errorf("%w: tso response is nil", errInvalidWitness)
	}
	return c.validateMonotoneWitness("tso", normalizedCount(requestedTSOCount(req)), resp.GetTimestamp(), resp.GetCount(), resp.GetCertGeneration(), resp.GetConsumedFrontier(), resp.GetObservedSealGeneration(), &c.tsoGen)
}

func (c *GRPCClient) validateMonotoneWitness(kind string, requestedCount, first, gotCount, certGeneration, consumedFrontier, observedSealGeneration uint64, generation *witnessGenerationFloor) error {
	if certGeneration == rootproto.ContinuationWitnessGenerationSuppressed {
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
	return c.advanceWitnessGenerationFloor(kind, certGeneration, observedSealGeneration, generation)
}

func (c *GRPCClient) validateMetadataWitnessGeneration(resp *coordpb.GetRegionByKeyResponse) error {
	certGeneration := resp.GetCertGeneration()
	if certGeneration == rootproto.ContinuationWitnessGenerationSuppressed {
		return fmt.Errorf("%w: get_region_by_key reply evidence suppressed", errInvalidWitness)
	}
	if certGeneration == rootproto.ContinuationWitnessGenerationAttached {
		if resp.GetServingClass() != coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE ||
			resp.GetSyncHealth() != coordpb.SyncHealth_SYNC_HEALTH_HEALTHY ||
			!resp.GetServedByLeader() ||
			resp.GetRootLag() != 0 ||
			resp.GetCatchUpState() != coordpb.CatchUpState_CATCH_UP_STATE_FRESH {
			return fmt.Errorf(
				"%w: get_region_by_key cert_generation=0 requires authoritative attached serving_class=%s sync_health=%s served_by_leader=%t root_lag=%d catch_up_state=%s",
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
	return c.advanceWitnessGenerationFloor("get_region_by_key", certGeneration, resp.GetObservedSealGeneration(), &c.metadataGen)
}

func (c *GRPCClient) advanceAttachedMetadataFloor(resp *coordpb.GetRegionByKeyResponse) error {
	c.verifyMu.Lock()
	defer c.verifyMu.Unlock()

	currentToken := resp.GetCurrentRootToken()
	if c.metadataAttached.hasCurrentToken && !metadataRootTokenSatisfied(currentToken, c.metadataAttached.currentToken) {
		return fmt.Errorf(
			"%w: get_region_by_key cert_generation=0 current_root_token regressed behind attached floor",
			errInvalidWitness,
		)
	}
	if c.metadataAttached.descriptorRevision != 0 &&
		resp.GetDescriptorRevision() != 0 &&
		resp.GetDescriptorRevision() < c.metadataAttached.descriptorRevision {
		return fmt.Errorf(
			"%w: get_region_by_key cert_generation=0 descriptor_revision=%d attached_floor=%d",
			errInvalidWitness,
			resp.GetDescriptorRevision(),
			c.metadataAttached.descriptorRevision,
		)
	}

	if currentToken != nil {
		c.metadataAttached.currentToken = proto.Clone(currentToken).(*coordpb.RootToken)
		c.metadataAttached.hasCurrentToken = true
	}
	if resp.GetDescriptorRevision() > c.metadataAttached.descriptorRevision {
		c.metadataAttached.descriptorRevision = resp.GetDescriptorRevision()
	}
	return nil
}

func (c *GRPCClient) advanceWitnessGenerationFloor(kind string, certGeneration, observedSealGeneration uint64, floor *witnessGenerationFloor) error {
	c.verifyMu.Lock()
	defer c.verifyMu.Unlock()
	if certGeneration == rootproto.ContinuationWitnessGenerationSuppressed {
		return fmt.Errorf("%w: %s cert_generation suppressed", errInvalidWitness, kind)
	}
	if observedSealGeneration > floor.sealedSeen {
		floor.sealedSeen = observedSealGeneration
	}
	if floor.sealedSeen != 0 && certGeneration <= floor.sealedSeen {
		return fmt.Errorf("%w: %s cert_generation=%d sealed_floor=%d", errStaleWitnessGeneration, kind, certGeneration, floor.sealedSeen)
	}
	if certGeneration < floor.maxSeen {
		return fmt.Errorf("%w: %s cert_generation=%d max_seen=%d", errStaleWitnessGeneration, kind, certGeneration, floor.maxSeen)
	}
	if certGeneration > floor.maxSeen {
		floor.maxSeen = certGeneration
	}
	return nil
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
