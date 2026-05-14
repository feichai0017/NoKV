// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const (
	defaultTSOCoalesceWindow     = 200 * time.Microsecond
	defaultTSOCoalesceMaxCount   = uint64(512)
	defaultTSOCoalesceRPCTimeout = 2 * time.Second
)

type tsoCoalescerConfig struct {
	window     time.Duration
	maxCount   uint64
	rpcTimeout time.Duration
}

func defaultTSOCoalescerConfig() tsoCoalescerConfig {
	return tsoCoalescerConfig{
		window:     defaultTSOCoalesceWindow,
		maxCount:   defaultTSOCoalesceMaxCount,
		rpcTimeout: defaultTSOCoalesceRPCTimeout,
	}
}

type tsoCoalescer struct {
	client     TSOClient
	window     time.Duration
	maxCount   uint64
	rpcTimeout time.Duration

	mu    sync.Mutex
	batch *tsoBatch

	requestsTotal  atomic.Uint64
	batchesTotal   atomic.Uint64
	directTotal    atomic.Uint64
	allocatedTotal atomic.Uint64
	canceledTotal  atomic.Uint64
}

type tsoBatch struct {
	requests []*tsoRequest
	count    uint64
	timer    *time.Timer
}

type tsoRequest struct {
	count  uint64
	result chan tsoResult
}

type tsoResult struct {
	first uint64
	err   error
}

func newTSOCoalescer(client TSOClient, cfg tsoCoalescerConfig) *tsoCoalescer {
	if cfg.window < 0 {
		cfg.window = 0
	}
	if cfg.maxCount == 0 {
		cfg.maxCount = defaultTSOCoalesceMaxCount
	}
	if cfg.rpcTimeout < 0 {
		cfg.rpcTimeout = 0
	}
	return &tsoCoalescer{
		client:     client,
		window:     cfg.window,
		maxCount:   cfg.maxCount,
		rpcTimeout: cfg.rpcTimeout,
	}
}

func (c *tsoCoalescer) Reserve(ctx context.Context, count uint64) (uint64, error) {
	ctx, err := normalizeTSOReserveInput(ctx, count)
	if err != nil {
		return 0, err
	}
	c.requestsTotal.Add(1)
	if c.window == 0 || count >= c.maxCount {
		return c.reserveDirect(ctx, count)
	}
	req := &tsoRequest{count: count, result: make(chan tsoResult, 1)}
	c.enqueue(req)
	select {
	case result := <-req.result:
		return result.first, result.err
	case <-ctx.Done():
		// The queued request may already be part of a coordinator TSO batch.
		// Leaving it in the batch can create a timestamp gap, but prevents
		// reusing a timestamp that was reserved for a canceled caller.
		c.canceledTotal.Add(1)
		return 0, ctx.Err()
	}
}

func (c *tsoCoalescer) ReserveImmediate(ctx context.Context, count uint64) (uint64, error) {
	ctx, err := normalizeTSOReserveInput(ctx, count)
	if err != nil {
		return 0, err
	}
	c.requestsTotal.Add(1)
	return c.reserveDirect(ctx, count)
}

func normalizeTSOReserveInput(ctx context.Context, count uint64) (context.Context, error) {
	if count == 0 {
		return nil, errTimestampCountRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ctx, nil
}

func (c *tsoCoalescer) reserveDirect(ctx context.Context, count uint64) (uint64, error) {
	c.directTotal.Add(1)
	resp, err := c.client.Tso(ctx, &coordpb.TsoRequest{Count: count})
	first, err := validateTSOResponse(resp, count, err)
	if err != nil {
		return 0, err
	}
	c.allocatedTotal.Add(count)
	return first, nil
}

func (c *tsoCoalescer) enqueue(req *tsoRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batch != nil && c.batch.count > 0 && req.count > c.maxCount-c.batch.count {
		batch := c.takeBatchLocked()
		go c.dispatch(batch)
	}
	if c.batch == nil {
		c.batch = c.newBatchLocked()
	}
	c.batch.requests = append(c.batch.requests, req)
	c.batch.count += req.count
	if c.batch.count >= c.maxCount {
		batch := c.takeBatchLocked()
		go c.dispatch(batch)
	}
}

func (c *tsoCoalescer) newBatchLocked() *tsoBatch {
	batch := &tsoBatch{}
	batch.timer = time.AfterFunc(c.window, func() {
		c.mu.Lock()
		if c.batch != batch {
			c.mu.Unlock()
			return
		}
		taken := c.takeBatchLocked()
		c.mu.Unlock()
		if taken != nil {
			go c.dispatch(taken)
		}
	})
	return batch
}

func (c *tsoCoalescer) takeBatchLocked() *tsoBatch {
	batch := c.batch
	c.batch = nil
	if batch != nil && batch.timer != nil {
		batch.timer.Stop()
	}
	return batch
}

func (c *tsoCoalescer) dispatch(batch *tsoBatch) {
	if batch == nil || batch.count == 0 {
		return
	}
	c.batchesTotal.Add(1)
	// A coalesced batch can contain many caller contexts. Binding the RPC to
	// one of them would let one canceled caller cancel timestamp reservations
	// for unrelated waiters; the batch uses its own timeout and canceled entries
	// are instead left as legal timestamp gaps.
	ctx := context.Background()
	var cancel context.CancelFunc
	if c.rpcTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.rpcTimeout)
		defer cancel()
	}
	resp, err := c.client.Tso(ctx, &coordpb.TsoRequest{Count: batch.count})
	first, err := validateTSOResponse(resp, batch.count, err)
	if err == nil {
		c.allocatedTotal.Add(batch.count)
	}
	var offset uint64
	for _, req := range batch.requests {
		result := tsoResult{err: err}
		if err == nil {
			result.first = first + offset
			offset += req.count
		}
		req.result <- result
	}
}

func validateTSOResponse(resp *coordpb.TsoResponse, requested uint64, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errNilTSOResponse
	}
	if resp.GetCount() != requested {
		return 0, errTSOCountMismatch(resp.GetCount(), requested)
	}
	if resp.GetTimestamp() == 0 {
		return 0, errZeroTSOTimestamp
	}
	return resp.GetTimestamp(), nil
}

func (c *tsoCoalescer) Stats() map[string]any {
	if c == nil {
		return map[string]any{
			"tso_coalesce_requests_total":  uint64(0),
			"tso_coalesce_batches_total":   uint64(0),
			"tso_direct_requests_total":    uint64(0),
			"tso_coalesce_allocated_total": uint64(0),
			"tso_coalesce_canceled_total":  uint64(0),
		}
	}
	return map[string]any{
		"tso_coalesce_requests_total":  c.requestsTotal.Load(),
		"tso_coalesce_batches_total":   c.batchesTotal.Load(),
		"tso_direct_requests_total":    c.directTotal.Load(),
		"tso_coalesce_allocated_total": c.allocatedTotal.Load(),
		"tso_coalesce_canceled_total":  c.canceledTotal.Load(),
	}
}
