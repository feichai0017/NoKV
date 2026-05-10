package flush

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubPayload stands in for a *memTable in tests; the runtime is generic
// over whatever payload callers attach.
type stubPayload struct{ id int }

func TestRuntimeEnqueueAndNext(t *testing.T) {
	rt := New[*stubPayload](1)
	defer func() { _ = rt.Close() }()

	payload := &stubPayload{id: 7}
	if err := rt.Enqueue(0, payload); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	task, ok := rt.Next()
	if !ok {
		t.Fatalf("expected task")
	}
	if task.Payload != payload {
		t.Fatalf("unexpected payload in task")
	}

	rt.MarkInstalled(task)
	rt.MarkDone(task)

	stats := rt.Stats()
	if stats.Pending != 0 {
		t.Fatalf("expected pending=0 got %d", stats.Pending)
	}
	if stats.Completed != 1 {
		t.Fatalf("expected completed=1 got %d", stats.Completed)
	}
}

func TestRuntimeNextBlocksUntilEnqueue(t *testing.T) {
	rt := New[*stubPayload](1)
	defer func() { _ = rt.Close() }()

	var wg sync.WaitGroup
	wg.Go(func() {
		task, ok := rt.Next()
		if !ok {
			t.Errorf("expected task")
			return
		}
		rt.MarkDone(task)
	})

	time.Sleep(10 * time.Millisecond)
	if err := rt.Enqueue(0, &stubPayload{id: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	wg.Wait()
}

func TestRuntimeRejectsAfterClose(t *testing.T) {
	rt := New[*stubPayload](1)
	if err := rt.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := rt.Enqueue(0, &stubPayload{id: 1}); err == nil {
		t.Fatalf("expected enqueue after close to fail")
	}
}

// TestRuntimePerShardSerialization is the regression for the bug
// the user spotted: with multiple workers + a single global queue,
// same-shard tasks could complete out of segment-id order. The
// runtime must guarantee that for any one shard, only one task is in
// flight at a time, even when many workers and many shards are active.
//
// Strategy: enqueue many tasks for the same shard, run a worker pool
// the size of the shard count, and assert no two workers are inside
// the simulated "flush" critical section for that shard simultaneously.
func TestRuntimePerShardSerialization(t *testing.T) {
	const (
		shardCount = 4
		workers    = 4
		perShard   = 32
	)
	rt := New[*stubPayload](shardCount)

	var inFlight [shardCount]atomic.Int32
	var maxInFlight [shardCount]atomic.Int32

	for s := range shardCount {
		for i := range perShard {
			if err := rt.Enqueue(s, &stubPayload{id: i}); err != nil {
				t.Fatalf("enqueue shard=%d i=%d: %v", s, i, err)
			}
		}
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				task, ok := rt.Next()
				if !ok {
					return
				}
				cur := inFlight[task.ShardID].Add(1)
				for {
					prev := maxInFlight[task.ShardID].Load()
					if cur <= prev || maxInFlight[task.ShardID].CompareAndSwap(prev, cur) {
						break
					}
				}
				time.Sleep(50 * time.Microsecond)
				inFlight[task.ShardID].Add(-1)
				rt.MarkInstalled(task)
				rt.MarkDone(task)
			}
		})
	}

	for rt.Stats().Pending != 0 {
		time.Sleep(time.Millisecond)
	}
	_ = rt.Close()
	wg.Wait()

	for s := range shardCount {
		if got := maxInFlight[s].Load(); got > 1 {
			t.Fatalf("shard %d had %d concurrent in-flight tasks; per-shard serialization invariant violated", s, got)
		}
	}
	if got := rt.Stats().Completed; got != int64(shardCount*perShard) {
		t.Fatalf("expected %d completed, got %d", shardCount*perShard, got)
	}
}

// TestRuntimeCrossShardParallelism is the positive corollary: across
// shards, tasks should run in parallel. Enqueue one task per shard;
// with one worker per shard, the simulated build phases must overlap
// in time.
func TestRuntimeCrossShardParallelism(t *testing.T) {
	const shardCount = 4
	rt := New[*stubPayload](shardCount)
	for s := range shardCount {
		if err := rt.Enqueue(s, &stubPayload{id: s}); err != nil {
			t.Fatalf("enqueue shard=%d: %v", s, err)
		}
	}

	var ready sync.WaitGroup
	ready.Add(shardCount)
	release := make(chan struct{})

	var workers sync.WaitGroup
	for range shardCount {
		workers.Go(func() {
			task, ok := rt.Next()
			if !ok {
				t.Errorf("expected task")
				return
			}
			ready.Done()
			<-release
			rt.MarkInstalled(task)
			rt.MarkDone(task)
		})
	}

	done := make(chan struct{})
	go func() {
		ready.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("workers failed to reach barrier; cross-shard parallelism not observed")
	}
	close(release)
	workers.Wait()
	_ = rt.Close()
}
