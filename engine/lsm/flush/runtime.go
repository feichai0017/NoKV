// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package flush owns the LSM flush queue: the per-shard FIFO that
// preserves WAL segment ordering while letting different shards flush
// in parallel.
package flush

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/metrics"
)

// Sentinel errors returned by Enqueue.
var (
	ErrNil    = errors.New("flush: runtime is nil")
	ErrClosed = errors.New("flush: runtime closed")
)

// Task is a single flush request. Payload is the typed value passed by the
// caller at Enqueue time (typically *memTable). ShardID drives per-shard
// serialization.
type Task[T any] struct {
	Payload    T
	ShardID    int
	queuedAt   time.Time
	buildStart time.Time
	installAt  time.Time
}

// Runtime is the flush queue, generic over the payload type.
//
// Sharded ordering invariant
// ──────────────────────────
// Each lsmShard rotates its active memtable independently. Recovery,
// WAL retention, and the per-shard highestFlushedSeg high-water mark
// all assume that *for one shard* the manifest LogEdits, the
// setLogPointer call, and the WAL.RemoveSegment call happen in WAL
// segment-id order. Cross-shard flushes can run in parallel; same-
// shard flushes must serialize.
//
// We achieve that with one queue per shard plus an inFlight flag per
// shard. A worker only picks up a task whose shard has no in-flight
// task; the inFlight bit is cleared by MarkDone, which then broadcasts
// to wake any worker waiting on cond. The result is "N workers across
// N shards run in parallel; tasks within one shard run strictly FIFO
// in segment-id order."
//
// The single-queue, multi-worker scheme that this replaced did NOT
// satisfy that ordering. With two memTables enqueued for one shard a
// fast worker could install seg=11 before a slow worker installed
// seg=10, advancing the per-shard high-water and the WAL retention
// mark; a crash between the two installs would lose seg=10's WAL
// entries on restart (manifest says they're flushed but no SST holds
// them).
type Runtime[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	shards []*shardState[T]

	pending       atomic.Int64
	queueLen      atomic.Int64
	activeCt      atomic.Int64
	waitSumNs     atomic.Int64
	waitCount     atomic.Int64
	waitLastNs    atomic.Int64
	waitMaxNs     atomic.Int64
	buildSumNs    atomic.Int64
	buildCount    atomic.Int64
	buildLastNs   atomic.Int64
	buildMaxNs    atomic.Int64
	releaseSumNs  atomic.Int64
	releaseCount  atomic.Int64
	releaseLastNs atomic.Int64
	releaseMaxNs  atomic.Int64
	completed     atomic.Int64
}

// shardState is the per-shard FIFO queue plus the "owned by a worker
// right now" flag that Next consults to enforce per-shard serialization.
type shardState[T any] struct {
	queue    []*Task[T]
	inFlight bool
}

// New constructs a Runtime sized for shardCount shards.
func New[T any](shardCount int) *Runtime[T] {
	if shardCount <= 0 {
		shardCount = 1
	}
	rt := &Runtime[T]{
		shards: make([]*shardState[T], shardCount),
	}
	for i := range rt.shards {
		rt.shards[i] = &shardState[T]{}
	}
	rt.cond = sync.NewCond(&rt.mu)
	return rt
}

// Close marks the runtime closed and wakes any blocked workers.
func (rt *Runtime[T]) Close() error {
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	rt.closed = true
	rt.mu.Unlock()
	rt.cond.Broadcast()
	return nil
}

// Enqueue appends a task for shardID. Returns ErrClosed if the runtime
// has been closed.
func (rt *Runtime[T]) Enqueue(shardID int, payload T) error {
	if rt == nil {
		return ErrNil
	}
	if shardID < 0 || shardID >= len(rt.shards) {
		return fmt.Errorf("flush: invalid shard id %d (shard count %d)", shardID, len(rt.shards))
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return ErrClosed
	}
	rt.shards[shardID].queue = append(rt.shards[shardID].queue, &Task[T]{
		Payload:  payload,
		ShardID:  shardID,
		queuedAt: time.Now(),
	})
	rt.pending.Add(1)
	rt.queueLen.Add(1)
	rt.cond.Signal()
	return nil
}

// Next returns the next eligible task: any shard whose queue is non-
// empty and whose previous task has marked done. Tasks within one
// shard are pulled in FIFO order; across shards, the iteration order
// is round-robin starting from shard 0 — fairness is not formally
// guaranteed but is observably good with the bounded shard count we
// run today (typically 4).
//
// Blocks until a task becomes eligible, the runtime is closed and
// drained, or another worker calls MarkDone and wakes us via cond.
func (rt *Runtime[T]) Next() (*Task[T], bool) {
	if rt == nil {
		return nil, false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for {
		if task := rt.pickEligibleLocked(); task != nil {
			task.buildStart = time.Now()
			if !task.queuedAt.IsZero() {
				waitNs := time.Since(task.queuedAt).Nanoseconds()
				rt.waitSumNs.Add(waitNs)
				rt.waitCount.Add(1)
				rt.waitLastNs.Store(waitNs)
				updateMaxInt64(&rt.waitMaxNs, waitNs)
			}
			rt.activeCt.Add(1)
			return task, true
		}
		if rt.closed && !rt.anyPendingLocked() {
			return nil, false
		}
		rt.cond.Wait()
	}
}

func (rt *Runtime[T]) pickEligibleLocked() *Task[T] {
	for _, s := range rt.shards {
		if s.inFlight || len(s.queue) == 0 {
			continue
		}
		task := s.queue[0]
		s.queue = s.queue[1:]
		s.inFlight = true
		rt.queueLen.Add(-1)
		return task
	}
	return nil
}

func (rt *Runtime[T]) anyPendingLocked() bool {
	for _, s := range rt.shards {
		if s.inFlight || len(s.queue) > 0 {
			return true
		}
	}
	return false
}

// MarkInstalled records that the SST for task is durable on disk.
func (rt *Runtime[T]) MarkInstalled(task *Task[T]) {
	if rt == nil || task == nil {
		return
	}
	if !task.buildStart.IsZero() {
		buildNs := time.Since(task.buildStart).Nanoseconds()
		rt.buildSumNs.Add(buildNs)
		rt.buildCount.Add(1)
		rt.buildLastNs.Store(buildNs)
		updateMaxInt64(&rt.buildMaxNs, buildNs)
	}
	task.installAt = time.Now()
}

// MarkDone clears the inFlight flag for the task's shard so other
// workers can pick up subsequent tasks from the same shard, and
// broadcasts in case a worker was blocked waiting for this shard to
// free up.
func (rt *Runtime[T]) MarkDone(task *Task[T]) {
	if rt == nil || task == nil {
		return
	}
	if !task.installAt.IsZero() {
		releaseNs := time.Since(task.installAt).Nanoseconds()
		rt.releaseSumNs.Add(releaseNs)
		rt.releaseCount.Add(1)
		rt.releaseLastNs.Store(releaseNs)
		updateMaxInt64(&rt.releaseMaxNs, releaseNs)
	}
	rt.mu.Lock()
	if task.ShardID >= 0 && task.ShardID < len(rt.shards) {
		rt.shards[task.ShardID].inFlight = false
	}
	rt.mu.Unlock()
	rt.activeCt.Add(-1)
	rt.pending.Add(-1)
	rt.completed.Add(1)
	rt.cond.Broadcast()
}

// Stats returns the current metrics snapshot.
func (rt *Runtime[T]) Stats() metrics.FlushMetrics {
	if rt == nil {
		return metrics.FlushMetrics{}
	}
	return metrics.FlushMetrics{
		Pending:       rt.pending.Load(),
		Queue:         rt.queueLen.Load(),
		Active:        rt.activeCt.Load(),
		WaitNs:        rt.waitSumNs.Load(),
		WaitCount:     rt.waitCount.Load(),
		WaitLastNs:    rt.waitLastNs.Load(),
		WaitMaxNs:     rt.waitMaxNs.Load(),
		BuildNs:       rt.buildSumNs.Load(),
		BuildCount:    rt.buildCount.Load(),
		BuildLastNs:   rt.buildLastNs.Load(),
		BuildMaxNs:    rt.buildMaxNs.Load(),
		ReleaseNs:     rt.releaseSumNs.Load(),
		ReleaseCount:  rt.releaseCount.Load(),
		ReleaseLastNs: rt.releaseLastNs.Load(),
		ReleaseMaxNs:  rt.releaseMaxNs.Load(),
		Completed:     rt.completed.Load(),
	}
}

func updateMaxInt64(target *atomic.Int64, val int64) {
	for {
		current := target.Load()
		if val <= current {
			return
		}
		if target.CompareAndSwap(current, val) {
			return
		}
	}
}
