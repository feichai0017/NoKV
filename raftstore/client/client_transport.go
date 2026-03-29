package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	"github.com/feichai0017/NoKV/pb"
)

type storeConn struct {
	addr        string
	dialTimeout time.Duration
	dialOpts    []grpc.DialOption

	mu     sync.Mutex
	conn   *grpc.ClientConn
	client pb.NoKVClient
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

func (c *Client) storeClient(ctx context.Context, storeID uint64) (pb.NoKVClient, error) {
	c.mu.RLock()
	if storeID == 0 {
		c.mu.RUnlock()
		return nil, errors.New("client: store id not set")
	}
	st, ok := c.stores[storeID]
	c.mu.RUnlock()
	if !ok || st == nil {
		return nil, fmt.Errorf("client: store %d not found", storeID)
	}
	return st.clientFor(ctx)
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
	return status.Code(err) == codes.Unavailable
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

func (st *storeConn) clientFor(ctx context.Context) (pb.NoKVClient, error) {
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
	st.client = pb.NewNoKVClient(conn)
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
