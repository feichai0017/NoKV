package client

import (
	"context"
	"errors"
	"fmt"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"maps"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// KeyConflictError represents prewrite-time key conflicts surfaced by the raft
// service. Callers can inspect the KeyErrors to resolve locks before retrying.
type KeyConflictError struct {
	Errors []*kvrpcpb.KeyError
}

func (e *KeyConflictError) Error() string {
	return fmt.Sprintf("client: prewrite key errors: %+v", e.Errors)
}

// RouteUnavailableError indicates that the client could not resolve a route
// for the requested key because the external resolver was unavailable or the
// lookup timed out. Callers may retry once control-plane connectivity recovers.
type RouteUnavailableError struct {
	Key []byte
	Err error
}

func (e *RouteUnavailableError) Error() string {
	if e == nil {
		return "client: route unavailable"
	}
	return fmt.Sprintf("client: route unavailable for key %q: %v", e.Key, e.Err)
}

func (e *RouteUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsRouteUnavailable(err error) bool {
	var target *RouteUnavailableError
	return errors.As(err, &target)
}

// RegionNotFoundError indicates that no region metadata currently covers the
// requested key.
type RegionNotFoundError struct {
	Key []byte
}

func (e *RegionNotFoundError) Error() string {
	if e == nil {
		return "client: region not found"
	}
	return fmt.Sprintf("client: region not found for key %q", e.Key)
}

func IsRegionNotFound(err error) bool {
	var target *RegionNotFoundError
	return errors.As(err, &target)
}

// StoreEndpoint describes a reachable store in the cluster.
type StoreEndpoint struct {
	StoreID uint64
	Addr    string
}

// RegionResolver resolves Region metadata for an arbitrary key. A Coordinator client
// implementation should satisfy this interface.
type RegionResolver interface {
	GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error)
	Close() error
}

// Config configures the NoKV distributed client.
type Config struct {
	Context            context.Context
	Stores             []StoreEndpoint
	RegionResolver     RegionResolver
	RouteLookupTimeout time.Duration
	DialTimeout        time.Duration
	DialOptions        []grpc.DialOption
	Retry              RetryPolicy
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
	routeLookupTimeout time.Duration
	retry              RetryPolicy
}

// New constructs a Client using the provided configuration.
func New(cfg Config) (*Client, error) {
	if len(cfg.Stores) == 0 {
		return nil, errors.New("client: at least one store endpoint required")
	}
	if cfg.RegionResolver == nil {
		return nil, errors.New("client: region resolver required")
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	routeLookupTimeout := cfg.RouteLookupTimeout
	if routeLookupTimeout <= 0 {
		routeLookupTimeout = 2 * time.Second
	}
	dialOpts := cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	stores := make(map[uint64]*storeConn, len(cfg.Stores))
	for _, endpoint := range cfg.Stores {
		if endpoint.StoreID == 0 || endpoint.Addr == "" {
			return nil, fmt.Errorf("client: invalid store endpoint %+v", endpoint)
		}
		stores[endpoint.StoreID] = &storeConn{
			addr:        endpoint.Addr,
			dialTimeout: dialTimeout,
			dialOpts:    append([]grpc.DialOption(nil), dialOpts...),
		}
	}
	retry := cfg.Retry
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 5
	}
	retry.RouteUnavailableBackoff = normalizeRetryBackoff(retry.RouteUnavailableBackoff, 20*time.Millisecond)
	retry.TransportUnavailableBackoff = normalizeRetryBackoff(retry.TransportUnavailableBackoff, 10*time.Millisecond)
	retry.RegionErrorBackoff = normalizeRetryBackoff(retry.RegionErrorBackoff, 0)
	return &Client{
		stores:             stores,
		regions:            make(map[uint64]*regionState),
		regionResolver:     cfg.RegionResolver,
		routeLookupTimeout: routeLookupTimeout,
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
