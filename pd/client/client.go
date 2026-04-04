package client

import (
	"context"
	"errors"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var errEmptyAddress = errors.New("pd client: empty address")
var errNoReachableAddress = errors.New("pd client: no reachable address")

const errNotLeaderPrefix = "pd not leader"

// Client defines the PD-lite control-plane RPC contract consumed by stores.
type Client interface {
	StoreHeartbeat(ctx context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error)
	RegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error)
	RegionLiveness(ctx context.Context, req *pdpb.RegionLivenessRequest) (*pdpb.RegionLivenessResponse, error)
	PublishRootEvent(ctx context.Context, req *pdpb.PublishRootEventRequest) (*pdpb.PublishRootEventResponse, error)
	RemoveRegion(ctx context.Context, req *pdpb.RemoveRegionRequest) (*pdpb.RemoveRegionResponse, error)
	GetRegionByKey(ctx context.Context, req *pdpb.GetRegionByKeyRequest) (*pdpb.GetRegionByKeyResponse, error)
	AllocID(ctx context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error)
	Tso(ctx context.Context, req *pdpb.TsoRequest) (*pdpb.TsoResponse, error)
	Close() error
}

// GRPCClient is a thin wrapper around generated pdpb.PDClient.
type GRPCClient struct {
	mu        sync.Mutex
	endpoints []grpcEndpoint
	preferred int
}

type grpcEndpoint struct {
	addr string
	conn *grpc.ClientConn
	pd   pdpb.PDClient
}

// NewGRPCClient dials a PD-lite endpoint and returns a ready client.
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
			addr: target,
			conn: conn,
			pd:   pdpb.NewPDClient(conn),
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

// StoreHeartbeat forwards store heartbeat RPC.
func (c *GRPCClient) StoreHeartbeat(ctx context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	return invokeRPC(c, retryableRead, func(pd pdpb.PDClient) (*pdpb.StoreHeartbeatResponse, error) {
		return pd.StoreHeartbeat(ctx, req)
	})
}

// RegionHeartbeat forwards region heartbeat RPC.
func (c *GRPCClient) RegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error) {
	return invokeRPC(c, retryableWrite, func(pd pdpb.PDClient) (*pdpb.RegionHeartbeatResponse, error) {
		return pd.RegionHeartbeat(ctx, req)
	})
}

// RegionLiveness forwards region liveness heartbeat RPC.
func (c *GRPCClient) RegionLiveness(ctx context.Context, req *pdpb.RegionLivenessRequest) (*pdpb.RegionLivenessResponse, error) {
	return invokeRPC(c, retryableRead, func(pd pdpb.PDClient) (*pdpb.RegionLivenessResponse, error) {
		return pd.RegionLiveness(ctx, req)
	})
}

// PublishRootEvent forwards explicit rooted event RPC.
func (c *GRPCClient) PublishRootEvent(ctx context.Context, req *pdpb.PublishRootEventRequest) (*pdpb.PublishRootEventResponse, error) {
	return invokeRPC(c, retryableWrite, func(pd pdpb.PDClient) (*pdpb.PublishRootEventResponse, error) {
		return pd.PublishRootEvent(ctx, req)
	})
}

// RemoveRegion forwards region removal RPC.
func (c *GRPCClient) RemoveRegion(ctx context.Context, req *pdpb.RemoveRegionRequest) (*pdpb.RemoveRegionResponse, error) {
	return invokeRPC(c, retryableWrite, func(pd pdpb.PDClient) (*pdpb.RemoveRegionResponse, error) {
		return pd.RemoveRegion(ctx, req)
	})
}

// GetRegionByKey forwards region lookup RPC.
func (c *GRPCClient) GetRegionByKey(ctx context.Context, req *pdpb.GetRegionByKeyRequest) (*pdpb.GetRegionByKeyResponse, error) {
	return invokeRPC(c, retryableRead, func(pd pdpb.PDClient) (*pdpb.GetRegionByKeyResponse, error) {
		return pd.GetRegionByKey(ctx, req)
	})
}

// AllocID forwards ID allocation RPC.
func (c *GRPCClient) AllocID(ctx context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	return invokeRPC(c, retryableWrite, func(pd pdpb.PDClient) (*pdpb.AllocIDResponse, error) {
		return pd.AllocID(ctx, req)
	})
}

// Tso forwards TSO allocation RPC.
func (c *GRPCClient) Tso(ctx context.Context, req *pdpb.TsoRequest) (*pdpb.TsoResponse, error) {
	return invokeRPC(c, retryableWrite, func(pd pdpb.PDClient) (*pdpb.TsoResponse, error) {
		return pd.Tso(ctx, req)
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
			return errors.New("pd client: grpc connection shutdown")
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

func invokeRPC[T any](c *GRPCClient, retryable func(error) bool, call func(pd pdpb.PDClient) (T, error)) (T, error) {
	var zero T
	if c == nil {
		return zero, errNoReachableAddress
	}
	endpoints := c.orderedEndpoints()
	if len(endpoints) == 0 {
		return zero, errNoReachableAddress
	}
	var lastErr error
	for i, endpoint := range endpoints {
		resp, err := call(endpoint.pd)
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
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

func retryableWrite(err error) bool {
	if retryableRead(err) {
		return true
	}
	return status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), errNotLeaderPrefix)
}
