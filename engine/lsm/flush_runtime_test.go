package lsm

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shardedMemTable is a tiny test helper that wires a memTable to a shard
// id without pulling in the full memtable / wal stack.
func shardedMemTable(shardID int) *memTable {
	return &memTable{shard: &lsmShard{id: shardID}}
}

func TestFlushRuntimeEnqueueAndNext(t *testing.T) {
	rt := newFlushRuntime(1)
	defer func() { _ = rt.close() }()

	mt := shardedMemTable(0)
	if err := rt.enqueue(mt); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	task, ok := rt.next()
	if !ok {
		t.Fatalf("expected task")
	}
	if task.memTable != mt {
		t.Fatalf("unexpected memtable in task")
	}

	rt.markInstalled(task)
	rt.markDone(task)

	stats := rt.stats()
	if stats.Pending != 0 {
		t.Fatalf("expected pending=0 got %d", stats.Pending)
	}
	if stats.Completed != 1 {
		t.Fatalf("expected completed=1 got %d", stats.Completed)
	}
}

func TestFlushRuntimeNextBlocksUntilEnqueue(t *testing.T) {
	rt := newFlushRuntime(1)
	defer func() { _ = rt.close() }()

	var wg sync.WaitGroup
	wg.Go(func() {
		task, ok := rt.next()
		if !ok {
			t.Errorf("expected task")
			return
		}
		rt.markDone(task)
	})

	time.Sleep(10 * time.Millisecond)
	if err := rt.enqueue(shardedMemTable(0)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	wg.Wait()
}

func TestFlushRuntimeRejectsAfterClose(t *testing.T) {
	rt := newFlushRuntime(1)
	if err := rt.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := rt.enqueue(shardedMemTable(0)); err == nil {
		t.Fatalf("expected enqueue after close to fail")
	}
}

// TestFlushRuntimePerShardSerialization is the regression for the bug
// the user spotted: with multiple workers + a single global queue,
// same-shard tasks could complete out of segment-id order. The new
// runtime must guarantee that for any one shard, only one task is in
// flight at a time, even when many workers and many shards are active.
//
// Strategy: enqueue many tasks for the same shard, run a worker pool
// the size of the shard count, and assert no two workers are inside
// the simulated "flush" critical section for that shard simultaneously.
func TestFlushRuntimePerShardSerialization(t *testing.T) {
	const (
		shardCount = 4
		workers    = 4
		perShard   = 32
	)
	rt := newFlushRuntime(shardCount)

	// Track in-flight count per shard inside the simulated flush body.
	// Strict per-shard serialization means the per-shard counter must
	// never exceed 1.
	var inFlight [shardCount]atomic.Int32
	var maxInFlight [shardCount]atomic.Int32

	for s := range shardCount {
		for i := range perShard {
			if err := rt.enqueue(shardedMemTable(s)); err != nil {
				t.Fatalf("enqueue shard=%d i=%d: %v", s, i, err)
			}
		}
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				task, ok := rt.next()
				if !ok {
					return
				}
				cur := inFlight[task.shardID].Add(1)
				for {
					prev := maxInFlight[task.shardID].Load()
					if cur <= prev || maxInFlight[task.shardID].CompareAndSwap(prev, cur) {
						break
					}
				}
				// Tiny burn so a same-shard race would have time to
				// observe the elevated counter.
				time.Sleep(50 * time.Microsecond)
				inFlight[task.shardID].Add(-1)
				rt.markInstalled(task)
				rt.markDone(task)
			}
		})
	}

	// Close once all enqueued tasks drain so the workers exit.
	for rt.stats().Pending != 0 {
		time.Sleep(time.Millisecond)
	}
	_ = rt.close()
	wg.Wait()

	for s := range shardCount {
		if got := maxInFlight[s].Load(); got > 1 {
			t.Fatalf("shard %d had %d concurrent in-flight tasks; per-shard serialization invariant violated", s, got)
		}
	}
	if got := rt.stats().Completed; got != int64(shardCount*perShard) {
		t.Fatalf("expected %d completed, got %d", shardCount*perShard, got)
	}
}

// TestFlushRuntimeCrossShardParallelism is the positive corollary:
// across shards, tasks should run in parallel. Enqueue one task per
// shard; with one worker per shard, the simulated build phases must
// overlap in time.
func TestFlushRuntimeCrossShardParallelism(t *testing.T) {
	const shardCount = 4
	rt := newFlushRuntime(shardCount)
	for s := range shardCount {
		if err := rt.enqueue(shardedMemTable(s)); err != nil {
			t.Fatalf("enqueue shard=%d: %v", s, err)
		}
	}

	// All workers must reach the barrier simultaneously, proving they
	// were able to grab tasks for distinct shards in parallel rather
	// than serialize.
	var ready sync.WaitGroup
	ready.Add(shardCount)
	release := make(chan struct{})

	var workers sync.WaitGroup
	for range shardCount {
		workers.Go(func() {
			task, ok := rt.next()
			if !ok {
				t.Errorf("expected task")
				return
			}
			ready.Done()
			<-release
			rt.markInstalled(task)
			rt.markDone(task)
		})
	}

	done := make(chan struct{})
	go func() {
		ready.Wait()
		close(done)
	}()
	select {
	case <-done:
		// All workers reached the barrier — cross-shard parallelism works.
	case <-time.After(2 * time.Second):
		t.Fatal("workers failed to reach barrier; cross-shard parallelism not observed")
	}
	close(release)
	workers.Wait()
	_ = rt.close()
}
