package client

import (
	"context"
	"errors"
	"fmt"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

type storeConn struct {
	addr        string
	resolvedAt  time.Time
	dialTimeout time.Duration
	dialOpts    []grpc.DialOption

	mu     sync.Mutex
	conn   *grpc.ClientConn
	client kvrpcpb.NoKVClient
}

type retryKind uint8

const (
	retryRouteUnavailable retryKind = iota
	retryTransportUnavailable
	retryRegionError
)

func dialStore(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			_ = conn.Close()
			return nil, ctx.Err()
		}
	}
}

func (c *Client) storeClient(ctx context.Context, storeID uint64) (kvrpcpb.NoKVClient, error) {
	if storeID == 0 {
		return nil, errStoreIDNotSet
	}
	st, err := c.storeConn(ctx, storeID)
	if err != nil {
		return nil, err
	}
	cl, err := st.clientFor(ctx)
	if err == nil {
		return cl, nil
	}
	c.invalidateStore(storeID, st)
	refreshed, refreshErr := c.storeConn(ctx, storeID)
	if refreshErr != nil {
		return nil, fmt.Errorf("%w; refresh store %d: %v", err, storeID, refreshErr)
	}
	return refreshed.clientFor(ctx)
}

func (c *Client) storeConn(ctx context.Context, storeID uint64) (*storeConn, error) {
	now := time.Now()
	c.mu.RLock()
	st := c.stores[storeID]
	revalidate := c.storeRevalidateIn
	c.mu.RUnlock()
	if st != nil && !storeConnExpired(st, now, revalidate) {
		return st, nil
	}
	next, err := c.resolveStoreConn(ctx, storeID, now)
	if err != nil {
		if st != nil {
			if errors.Is(err, errStoreUnavailable) {
				c.invalidateStore(storeID, st)
				return nil, err
			}
			c.deferStoreRevalidation(storeID, st, now)
			return st, nil
		}
		return nil, err
	}
	c.mu.Lock()
	existing := c.stores[storeID]
	if existing != nil && existing != st && !storeConnExpired(existing, now, revalidate) {
		c.mu.Unlock()
		_ = next.close()
		return existing, nil
	}
	if st != nil && existing == st && st.addr == next.addr {
		st.resolvedAt = next.resolvedAt
		c.mu.Unlock()
		_ = next.close()
		return st, nil
	}
	c.stores[storeID] = next
	c.mu.Unlock()
	if existing != nil && existing != next {
		_ = existing.close()
	}
	return next, nil
}

func (c *Client) resolveStoreConn(ctx context.Context, storeID uint64, now time.Time) (*storeConn, error) {
	if c.storeResolver == nil {
		return nil, errMissingStoreResolver
	}
	resp, err := c.storeResolver.GetStore(ctx, &coordpb.GetStoreRequest{StoreId: storeID})
	if err != nil {
		return nil, fmt.Errorf("client: resolve store %d: %w", storeID, err)
	}
	if resp == nil || resp.GetNotFound() || resp.GetStore() == nil {
		return nil, fmt.Errorf("client: store %d not found", storeID)
	}
	info := resp.GetStore()
	if info.GetStoreId() != storeID {
		return nil, fmt.Errorf("client: resolved store id %d != requested %d", info.GetStoreId(), storeID)
	}
	if info.GetState() == coordpb.StoreState_STORE_STATE_UNKNOWN ||
		info.GetState() == coordpb.StoreState_STORE_STATE_DOWN ||
		info.GetState() == coordpb.StoreState_STORE_STATE_TOMBSTONE {
		return nil, fmt.Errorf("%w: store %d", errStoreUnavailable, storeID)
	}
	if info.GetClientAddr() == "" {
		return nil, fmt.Errorf("client: store %d has empty client address", storeID)
	}
	next := &storeConn{
		addr:        info.GetClientAddr(),
		resolvedAt:  now,
		dialTimeout: c.dialTimeout,
		dialOpts:    append([]grpc.DialOption(nil), c.dialOpts...),
	}
	return next, nil
}

func storeConnExpired(st *storeConn, now time.Time, interval time.Duration) bool {
	if st == nil || interval <= 0 {
		return false
	}
	if st.resolvedAt.IsZero() {
		return true
	}
	return !st.resolvedAt.Add(interval).After(now)
}

func (c *Client) deferStoreRevalidation(storeID uint64, expected *storeConn, now time.Time) {
	c.mu.Lock()
	if current := c.stores[storeID]; current == expected && current != nil {
		current.resolvedAt = now
	}
	c.mu.Unlock()
}

func (c *Client) invalidateStore(storeID uint64, expected *storeConn) {
	c.mu.Lock()
	if current := c.stores[storeID]; current == expected {
		delete(c.stores, storeID)
	}
	c.mu.Unlock()
	if expected != nil {
		_ = expected.close()
	}
}

func normalizeRetryBackoff(backoff, fallback time.Duration) time.Duration {
	if backoff < 0 {
		return 0
	}
	if backoff == 0 {
		return fallback
	}
	return backoff
}

func isTransportUnavailable(err error) bool {
	return errors.Is(err, errStoreUnavailable) || status.Code(err) == codes.Unavailable
}

func (c *Client) waitRetry(ctx context.Context, attempt int, kind retryKind) error {
	if attempt+1 >= c.retry.MaxAttempts {
		return nil
	}
	var backoff time.Duration
	switch kind {
	case retryRouteUnavailable:
		backoff = c.retry.RouteUnavailableBackoff
	case retryTransportUnavailable:
		backoff = c.retry.TransportUnavailableBackoff
	case retryRegionError:
		backoff = c.retry.RegionErrorBackoff
	default:
		backoff = 0
	}
	if backoff <= 0 {
		return nil
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

func normalizeRPCError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == 0 {
		return err
	}
	return err
}

func (st *storeConn) clientFor(ctx context.Context) (kvrpcpb.NoKVClient, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.client != nil && st.conn != nil && st.conn.GetState() != connectivity.Shutdown {
		return st.client, nil
	}
	if st.conn != nil {
		_ = st.conn.Close()
		st.conn = nil
		st.client = nil
	}
	dialCtx, cancel := contextWithTimeout(ctx, st.dialTimeout)
	defer cancel()
	conn, err := dialStore(dialCtx, st.addr, st.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", st.addr, err)
	}
	st.conn = conn
	st.client = kvrpcpb.NewNoKVClient(conn)
	return st.client, nil
}

func (st *storeConn) close() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.conn == nil {
		return nil
	}
	err := st.conn.Close()
	st.conn = nil
	st.client = nil
	return err
}
