package client

import (
	"context"
	"fmt"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"maps"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RegionResolver resolves Region metadata for an arbitrary key. A Coordinator client
// implementation should satisfy this interface.
type RegionResolver interface {
	GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error)
	Close() error
}

// StoreResolver resolves runtime store endpoints from the control plane.
type StoreResolver interface {
	GetStore(ctx context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error)
}

// Config configures the NoKV distributed client.
type Config struct {
	Context            context.Context
	RegionResolver     RegionResolver
	StoreResolver      StoreResolver
	RouteLookupTimeout time.Duration
	// StoreRevalidateInterval controls how often cached store endpoints are
	// checked against Coordinator. The default is 30s.
	StoreRevalidateInterval time.Duration
	DialTimeout             time.Duration
	DialOptions             []grpc.DialOption
	Retry                   RetryPolicy
}

// RetryPolicy defines retry budgets and backoff for client-side retries.
type RetryPolicy struct {
	MaxAttempts                 int
	RouteUnavailableBackoff     time.Duration
	TransportUnavailableBackoff time.Duration
	RegionErrorBackoff          time.Duration
}

// Client provides Region-aware helpers for NoKV RPCs, including 2PC.
type Client struct {
	mu                 sync.RWMutex
	stores             map[uint64]*storeConn
	regions            map[uint64]*regionState
	regionIndex        []regionRange
	regionResolver     RegionResolver
	storeResolver      StoreResolver
	routeLookupTimeout time.Duration
	storeRevalidateIn  time.Duration
	dialTimeout        time.Duration
	dialOpts           []grpc.DialOption
	retry              RetryPolicy
}

// New constructs a Client using the provided configuration.
func New(cfg Config) (*Client, error) {
	if cfg.RegionResolver == nil {
		return nil, errMissingRegionResolver
	}
	if cfg.StoreResolver == nil {
		return nil, errMissingStoreResolver
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	routeLookupTimeout := cfg.RouteLookupTimeout
	if routeLookupTimeout <= 0 {
		routeLookupTimeout = 2 * time.Second
	}
	storeRevalidateIn := cfg.StoreRevalidateInterval
	if storeRevalidateIn <= 0 {
		storeRevalidateIn = 30 * time.Second
	}
	dialOpts := cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	retry := cfg.Retry
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 5
	}
	retry.RouteUnavailableBackoff = normalizeRetryBackoff(retry.RouteUnavailableBackoff, 20*time.Millisecond)
	retry.TransportUnavailableBackoff = normalizeRetryBackoff(retry.TransportUnavailableBackoff, 10*time.Millisecond)
	retry.RegionErrorBackoff = normalizeRetryBackoff(retry.RegionErrorBackoff, 0)
	return &Client{
		stores:             make(map[uint64]*storeConn),
		regions:            make(map[uint64]*regionState),
		regionResolver:     cfg.RegionResolver,
		storeResolver:      cfg.StoreResolver,
		routeLookupTimeout: routeLookupTimeout,
		storeRevalidateIn:  storeRevalidateIn,
		dialTimeout:        dialTimeout,
		dialOpts:           append([]grpc.DialOption(nil), dialOpts...),
		retry:              retry,
	}, nil
}

// Close terminates outstanding store connections.
func (c *Client) Close() error {
	var first error
	c.mu.RLock()
	stores := make(map[uint64]*storeConn, len(c.stores))
	maps.Copy(stores, c.stores)
	resolver := c.regionResolver
	c.mu.RUnlock()
	for id, st := range stores {
		if st == nil {
			continue
		}
		if err := st.close(); err != nil && first == nil {
			first = fmt.Errorf("client: close store %d: %w", id, err)
		}
	}
	if resolver != nil {
		if err := resolver.Close(); err != nil && first == nil {
			first = fmt.Errorf("client: close region resolver: %w", err)
		}
	}
	return first
}
