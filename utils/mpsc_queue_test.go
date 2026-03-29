package utils

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMPSCQueueBasic(t *testing.T) {
	q := NewMPSCQueue[int](4)
	if !q.Push(1) || !q.Push(2) || !q.Push(3) || !q.Push(4) {
		t.Fatalf("expected pushes to succeed")
	}
	if q.ReservedLen() != 4 || q.Cap() < 4 {
		t.Fatalf("unexpected len/cap %d/%d", q.ReservedLen(), q.Cap())
	}
	for i := 1; i <= 4; i++ {
		v, ok := q.TryPop()
		if !ok || v != i {
			t.Fatalf("pop got %v %v at %d", v, ok, i)
		}
	}
	if _, ok := q.TryPop(); ok {
		t.Fatalf("expected empty try-pop")
	}
	if !q.Close() {
		t.Fatalf("close failed")
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected closed drained queue to return false")
	}
	if q.Push(5) {
		t.Fatalf("push should fail after close")
	}
}

func TestMPSCQueueConcurrent(t *testing.T) {
	q := NewMPSCQueue[int](128)
	const (
		producers = 8
		perProd   = 2000
		total     = producers * perProd
	)
	var produced atomic.Int64
	var consumed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := range producers {
		go func(id int) {
			defer wg.Done()
			base := id * perProd
			for i := range perProd {
				if !q.Push(base + i) {
					t.Errorf("push failed before close")
					return
				}
				produced.Add(1)
			}
		}(p)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, ok := q.Pop()
			if !ok {
				return
			}
			consumed.Add(1)
		}
	}()
	wg.Wait()
	q.Close()
	<-done
	if got := produced.Load(); got != total {
		t.Fatalf("produced %d, want %d", got, total)
	}
	if got := consumed.Load(); got != total {
		t.Fatalf("consumed %d, want %d", got, total)
	}
}

func TestMPSCQueueCloseUnblocksFullProducer(t *testing.T) {
	q := NewMPSCQueue[int](2)
	if !q.Push(1) || !q.Push(2) {
		t.Fatalf("initial fill failed")
	}
	done := make(chan bool, 1)
	go func() {
		done <- q.Push(3)
	}()
	for range 100 {
		runtime.Gosched()
	}
	if !q.Close() {
		t.Fatalf("close failed")
	}
	if ok := <-done; ok {
		t.Fatalf("expected blocked producer to fail after close")
	}
}

func TestMPSCQueueCloseRaceDrainsExactlyOnce(t *testing.T) {
	q := NewMPSCQueue[int](64)
	const (
		producers = 8
		perProd   = 1000
		total     = producers * perProd
	)

	var (
		wg       sync.WaitGroup
		consumed atomic.Int64
		seenMu   sync.Mutex
		seen     = make(map[int]struct{}, total)
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			v, ok := q.Pop()
			if !ok {
				return
			}
			seenMu.Lock()
			if _, exists := seen[v]; exists {
				seenMu.Unlock()
				t.Errorf("duplicate value %d", v)
				return
			}
			seen[v] = struct{}{}
			seenMu.Unlock()
			consumed.Add(1)
		}
	}()

	wg.Add(producers)
	for p := range producers {
		go func(id int) {
			defer wg.Done()
			base := id * perProd
			for i := range perProd {
				if !q.Push(base + i) {
					return
				}
				if i%97 == 0 {
					runtime.Gosched()
				}
			}
		}(p)
	}

	go func() {
		for range 200 {
			runtime.Gosched()
		}
		q.Close()
	}()

	wg.Wait()
	q.Close()
	<-done

	seenMu.Lock()
	defer seenMu.Unlock()
	if got := consumed.Load(); got != int64(len(seen)) {
		t.Fatalf("consumed=%d seen=%d mismatch", got, len(seen))
	}
	if len(seen) == 0 {
		t.Fatalf("expected some drained items")
	}
}

func TestMPSCQueueExactlyOnceDrain(t *testing.T) {
	const (
		producers = 6
		perProd   = 1500
		total     = producers * perProd
	)
	q := NewMPSCQueue[int](total)

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := range producers {
		go func(id int) {
			defer wg.Done()
			base := id * perProd
			for i := range perProd {
				if !q.Push(base + i) {
					t.Errorf("unexpected push failure before close")
					return
				}
			}
		}(p)
	}

	wg.Wait()
	q.Close()

	seen := make([]bool, total)
	count := 0
	for {
		v, ok := q.Pop()
		if !ok {
			break
		}
		if v < 0 || v >= total {
			t.Fatalf("value out of range: %d", v)
		}
		if seen[v] {
			t.Fatalf("duplicate value: %d", v)
		}
		seen[v] = true
		count++
	}
	if count != total {
		t.Fatalf("count=%d want=%d", count, total)
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("missing value: %d", i)
		}
	}
}

func TestMPSCQueueRejectsConcurrentConsumers(t *testing.T) {
	q := NewMPSCQueue[int](2)
	if !q.Push(1) {
		t.Fatalf("push failed")
	}

	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		q.acquireConsumer()
		close(started)
		<-release
		q.releaseConsumer()
	}()
	<-started

	defer close(release)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected concurrent consumer panic")
		}
	}()
	_, _ = q.TryPop()
}

func TestMPSCQueueDrainReady(t *testing.T) {
	q := NewMPSCQueue[int](8)
	for i := 1; i <= 4; i++ {
		if !q.Push(i) {
			t.Fatalf("push %d failed", i)
		}
	}
	c := q.AcquireConsumer()
	defer c.Close()

	var got []int
	n := c.DrainReady(3, func(v int) bool {
		got = append(got, v)
		return true
	})
	if n != 3 {
		t.Fatalf("drained %d, want 3", n)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("unexpected drain order: %v", got)
	}
	v, ok := c.Pop()
	if !ok || v != 4 {
		t.Fatalf("pop after drain got %v %v", v, ok)
	}
}

func TestMPSCQueueDrainReadyStopsWhenCallbackStops(t *testing.T) {
	q := NewMPSCQueue[int](8)
	for i := 1; i <= 4; i++ {
		if !q.Push(i) {
			t.Fatalf("push %d failed", i)
		}
	}
	c := q.AcquireConsumer()
	defer c.Close()

	var got []int
	n := c.DrainReady(4, func(v int) bool {
		got = append(got, v)
		return v < 2
	})
	if n != 2 {
		t.Fatalf("drained %d, want 2", n)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("unexpected drained values: %v", got)
	}
	v, ok := c.Pop()
	if !ok || v != 3 {
		t.Fatalf("pop after callback stop got %v %v", v, ok)
	}
}
