// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type recordingTSOClient struct {
	mu       sync.Mutex
	next     uint64
	requests []uint64
}

func (c *recordingTSOClient) Tso(_ context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next == 0 {
		c.next = 100
	}
	count := req.GetCount()
	first := c.next
	c.next += count
	c.requests = append(c.requests, count)
	return &coordpb.TsoResponse{Timestamp: first, Count: count}, nil
}

func (c *recordingTSOClient) requestCounts() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.requests...)
}

type blockingTSOClient struct {
	called chan struct{}
}

func (c *blockingTSOClient) Tso(ctx context.Context, _ *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	close(c.called)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestTSOCoalescerBatchesConcurrentRequests(t *testing.T) {
	client := &recordingTSOClient{}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{window: 20 * time.Millisecond, maxCount: 64})

	const requests = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]uint64, requests)
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ts, err := coalescer.Reserve(context.Background(), 1)
			require.NoError(t, err)
			results[i] = ts
		}(i)
	}
	close(start)
	wg.Wait()

	counts := client.requestCounts()
	require.Len(t, counts, 1)
	require.Equal(t, uint64(requests), counts[0])
	slices.Sort(results)
	for i, ts := range results {
		require.Equal(t, uint64(100+i), ts)
	}
	stats := coalescer.Stats()
	require.Equal(t, uint64(requests), stats["tso_coalesce_requests_total"])
	require.Equal(t, uint64(1), stats["tso_coalesce_batches_total"])
	require.Equal(t, uint64(0), stats["tso_direct_requests_total"])
	require.Equal(t, uint64(requests), stats["tso_coalesce_allocated_total"])
	require.Equal(t, uint64(0), stats["tso_coalesce_canceled_total"])
}

func TestTSOCoalescerFlushesAtMaxCount(t *testing.T) {
	client := &recordingTSOClient{}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{window: time.Minute, maxCount: 3})

	var wg sync.WaitGroup
	results := make([]uint64, 3)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ts, err := coalescer.Reserve(context.Background(), 1)
			require.NoError(t, err)
			results[i] = ts
		}(i)
	}
	wg.Wait()

	require.Equal(t, []uint64{3}, client.requestCounts())
	slices.Sort(results)
	require.Equal(t, []uint64{100, 101, 102}, results)
}

func TestTSOCoalescerKeepsBatchWithinMaxCount(t *testing.T) {
	client := &recordingTSOClient{}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{window: 20 * time.Millisecond, maxCount: 3})

	firstCh := make(chan uint64, 1)
	go func() {
		ts, err := coalescer.Reserve(context.Background(), 2)
		require.NoError(t, err)
		firstCh <- ts
	}()
	require.Eventually(t, func() bool {
		coalescer.mu.Lock()
		defer coalescer.mu.Unlock()
		return coalescer.batch != nil && coalescer.batch.count == 2
	}, time.Second, time.Millisecond)

	second, err := coalescer.Reserve(context.Background(), 2)
	require.NoError(t, err)
	first := <-firstCh

	require.Equal(t, uint64(100), first)
	require.Equal(t, uint64(102), second)
	require.Equal(t, []uint64{2, 2}, client.requestCounts())
}

func TestTSOCoalescerCanceledRequestLeavesTimestampGap(t *testing.T) {
	client := &recordingTSOClient{}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{window: 30 * time.Millisecond, maxCount: 64})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := coalescer.Reserve(ctx, 1)
		errCh <- err
	}()
	require.Eventually(t, func() bool {
		return coalescer.Stats()["tso_coalesce_requests_total"] == uint64(1)
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	ts, err := coalescer.Reserve(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(101), ts)
	require.Equal(t, []uint64{2}, client.requestCounts())

	stats := coalescer.Stats()
	require.Equal(t, uint64(2), stats["tso_coalesce_requests_total"])
	require.Equal(t, uint64(1), stats["tso_coalesce_batches_total"])
	require.Equal(t, uint64(0), stats["tso_direct_requests_total"])
	require.Equal(t, uint64(2), stats["tso_coalesce_allocated_total"])
	require.Equal(t, uint64(1), stats["tso_coalesce_canceled_total"])
}

func TestTSOCoalescerDirectLargeRequest(t *testing.T) {
	client := &recordingTSOClient{}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{window: time.Minute, maxCount: 4})

	ts, err := coalescer.Reserve(context.Background(), 8)
	require.NoError(t, err)
	require.Equal(t, uint64(100), ts)
	require.Equal(t, []uint64{8}, client.requestCounts())
	stats := coalescer.Stats()
	require.Equal(t, uint64(1), stats["tso_coalesce_requests_total"])
	require.Equal(t, uint64(0), stats["tso_coalesce_batches_total"])
	require.Equal(t, uint64(1), stats["tso_direct_requests_total"])
	require.Equal(t, uint64(8), stats["tso_coalesce_allocated_total"])
}

func TestTSOCoalescerImmediateBypassesOpenBatch(t *testing.T) {
	client := &recordingTSOClient{}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{window: time.Minute, maxCount: 2})

	firstCh := make(chan uint64, 1)
	go func() {
		ts, err := coalescer.Reserve(context.Background(), 1)
		require.NoError(t, err)
		firstCh <- ts
	}()
	require.Eventually(t, func() bool {
		coalescer.mu.Lock()
		defer coalescer.mu.Unlock()
		return coalescer.batch != nil && coalescer.batch.count == 1
	}, time.Second, time.Millisecond)

	immediate, err := coalescer.ReserveImmediate(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(100), immediate)
	require.Equal(t, []uint64{1}, client.requestCounts())

	second, err := coalescer.Reserve(context.Background(), 1)
	require.NoError(t, err)
	first := <-firstCh

	require.Equal(t, uint64(101), first)
	require.Equal(t, uint64(102), second)
	require.Equal(t, []uint64{1, 2}, client.requestCounts())
	stats := coalescer.Stats()
	require.Equal(t, uint64(1), stats["tso_coalesce_batches_total"])
	require.Equal(t, uint64(1), stats["tso_direct_requests_total"])
}

func TestTSOCoalescerBatchedRPCTimeoutBoundsHungCoordinator(t *testing.T) {
	client := &blockingTSOClient{called: make(chan struct{})}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{
		window:     time.Millisecond,
		maxCount:   4,
		rpcTimeout: 5 * time.Millisecond,
	})

	_, err := coalescer.Reserve(context.Background(), 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Eventually(t, func() bool {
		select {
		case <-client.called:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	stats := coalescer.Stats()
	require.Equal(t, uint64(1), stats["tso_coalesce_requests_total"])
	require.Equal(t, uint64(1), stats["tso_coalesce_batches_total"])
	require.Equal(t, uint64(0), stats["tso_direct_requests_total"])
	require.Equal(t, uint64(0), stats["tso_coalesce_allocated_total"])
}

func TestTSOCoalescerRejectsZeroCount(t *testing.T) {
	client := &recordingTSOClient{}
	coalescer := newTSOCoalescer(client, tsoCoalescerConfig{window: time.Minute, maxCount: 4})

	_, err := coalescer.Reserve(context.Background(), 0)
	require.ErrorIs(t, err, errTimestampCountRequired)
	_, err = coalescer.ReserveImmediate(context.Background(), 0)
	require.ErrorIs(t, err, errTimestampCountRequired)
	require.Empty(t, client.requestCounts())
}
