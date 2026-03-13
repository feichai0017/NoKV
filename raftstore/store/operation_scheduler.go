package store

import (
	"sync"
	"time"
)

type operationApplier func(Operation) bool

type operationScheduler struct {
	input     chan Operation
	stop      chan struct{}
	wg        sync.WaitGroup
	cooldown  time.Duration
	interval  time.Duration
	burst     int
	mu        sync.Mutex
	pending   map[operationKey]struct{}
	lastApply map[operationKey]time.Time
	apply     operationApplier
}

func newOperationScheduler(queueSize int, interval, cooldown time.Duration, burst int, apply operationApplier) *operationScheduler {
	os := &operationScheduler{
		cooldown:  cooldown,
		interval:  interval,
		burst:     burst,
		pending:   make(map[operationKey]struct{}),
		lastApply: make(map[operationKey]time.Time),
		apply:     apply,
	}
	if queueSize > 0 {
		os.input = make(chan Operation, queueSize)
		os.stop = make(chan struct{})
		os.wg.Add(1)
		go os.worker()
	}
	return os
}

func (os *operationScheduler) stopLoop() {
	if os == nil || os.stop == nil {
		return
	}
	close(os.stop)
	os.wg.Wait()
}

func (os *operationScheduler) enqueue(op Operation) {
	if os == nil {
		return
	}
	if op.Type == OperationNone || op.Region == 0 {
		return
	}
	if os.input == nil {
		os.execute(op)
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	os.mu.Lock()
	if _, exists := os.pending[key]; exists {
		os.mu.Unlock()
		return
	}
	os.pending[key] = struct{}{}
	os.mu.Unlock()
	select {
	case os.input <- op:
	default:
		os.mu.Lock()
		delete(os.pending, key)
		os.mu.Unlock()
	}
}

func (os *operationScheduler) worker() {
	defer os.wg.Done()
	interval := os.interval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	type scheduledOp struct {
		op    Operation
		ready time.Time
	}
	var pending []scheduledOp
	for {
		select {
		case <-os.stop:
			return
		case op := <-os.input:
			pending = append(pending, scheduledOp{op: op, ready: os.nextReadyTime(op)})
		case <-ticker.C:
			now := time.Now()
			limit := os.burst
			if limit <= 0 {
				limit = len(pending)
			}
			applied := 0
			var remaining []scheduledOp
			for _, item := range pending {
				if limit > 0 && applied >= limit {
					remaining = append(remaining, item)
					continue
				}
				if !item.ready.IsZero() && item.ready.After(now) {
					remaining = append(remaining, item)
					continue
				}
				if os.execute(item.op) {
					os.markApplied(item.op, now)
					applied++
				}
			}
			pending = remaining
		}
	}
}

func (os *operationScheduler) execute(op Operation) bool {
	if os == nil || os.apply == nil {
		return false
	}
	return os.apply(op)
}

func (os *operationScheduler) nextReadyTime(op Operation) time.Time {
	if os == nil {
		return time.Time{}
	}
	cooldown := os.cooldown
	if cooldown <= 0 {
		return time.Time{}
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	os.mu.Lock()
	defer os.mu.Unlock()
	last := os.lastApply[key]
	if last.IsZero() {
		return time.Time{}
	}
	return last.Add(cooldown)
}

func (os *operationScheduler) markApplied(op Operation, ts time.Time) {
	if os == nil {
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	os.mu.Lock()
	if ts.IsZero() {
		delete(os.lastApply, key)
	} else {
		os.lastApply[key] = ts
	}
	delete(os.pending, key)
	os.mu.Unlock()
}
