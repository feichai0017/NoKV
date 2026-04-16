package lsm

import (
	"sync"
	"testing"
	"time"
)

func TestFlushRuntimeEnqueueAndNext(t *testing.T) {
	rt := newFlushRuntime()
	defer func() { _ = rt.close() }()

	mt := &memTable{}
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
	rt := newFlushRuntime()
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
	if err := rt.enqueue(&memTable{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	wg.Wait()
}

func TestFlushRuntimeRejectsAfterClose(t *testing.T) {
	rt := newFlushRuntime()
	if err := rt.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := rt.enqueue(&memTable{}); err == nil {
		t.Fatalf("expected enqueue after close to fail")
	}
}
