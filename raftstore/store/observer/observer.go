// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package observer owns the post-apply event fanout: a Runtime
// dispatches Event values to registered Observers. Dispatch is
// non-blocking; slow observers drop events instead of stalling raft
// apply, with a periodic log on the dropped counter.
package observer

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

// Sentinel errors returned by Runtime.Register.
var (
	ErrNilObserver = errors.New("raftstore/observer: nil observer")
	ErrClosed      = errors.New("raftstore/observer: runtime closed")
)

// defaultBuffer is the per-observer channel capacity used when the
// caller passes a non-positive value to Register.
const defaultBuffer = 256

// Source identifies which committed raft command made keys visible.
type Source uint8

const (
	// SourceCommit covers regular write commits (CMD_COMMIT and the
	// CMD_TRY_ATOMIC_MUTATE one-phase commit that skipped two-phase commit).
	SourceCommit Source = iota + 1
	// SourceResolveLock covers commits that materialise via lock
	// resolution (CMD_RESOLVE_LOCK).
	SourceResolveLock
)

// Event is emitted after a raft command is successfully applied and
// makes one or more MVCC keys visible. Observers must treat the event
// as immutable.
type Event struct {
	RegionID      uint64
	Term          uint64
	Index         uint64
	Source        Source
	CommitVersion uint64
	Keys          [][]byte
	AtomicMutate  bool
}

// Observer consumes post-apply events. Runtime dispatch is non-blocking;
// a slow observer drops events instead of blocking raft apply.
type Observer interface {
	OnApply(Event)
}

// Registration unregisters a previously registered observer. Calling
// Close more than once is a no-op.
type Registration interface {
	Close()
}

// Runtime owns the registered observer set and the dispatch goroutines.
type Runtime struct {
	mu        sync.RWMutex
	next      uint64
	observers map[uint64]*registered
	dropped   atomic.Uint64
	closed    bool
}

type registered struct {
	id       uint64
	runtime  *Runtime
	observer Observer
	ch       chan Event
	once     sync.Once
}

// New constructs an empty Runtime.
func New() *Runtime {
	return &Runtime{observers: make(map[uint64]*registered)}
}

// Register starts dispatching to o using a buffered channel of the given
// capacity. Capacity <= 0 selects the default. Returns a Registration
// that the caller closes to stop dispatch.
func (r *Runtime) Register(o Observer, capacity int) (Registration, error) {
	if o == nil {
		return nil, ErrNilObserver
	}
	if capacity <= 0 {
		capacity = defaultBuffer
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	r.next++
	reg := &registered{
		id:       r.next,
		runtime:  r,
		observer: o,
		ch:       make(chan Event, capacity),
	}
	r.observers[reg.id] = reg
	go reg.run()
	return reg, nil
}

// Emit dispatches ev to every registered observer. Observers that cannot
// keep up have their events dropped; the dropped counter is incremented
// and a periodic log fires.
func (r *Runtime) Emit(ev Event) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}
	for _, obs := range r.observers {
		select {
		case obs.ch <- ev:
		default:
			dropped := r.dropped.Add(1)
			if dropped == 1 || dropped%1024 == 0 {
				log.Printf("raftstore/observer: dropped events total=%d", dropped)
			}
		}
	}
}

// Dropped returns the cumulative count of events that could not be
// delivered because some observer's channel was full.
func (r *Runtime) Dropped() uint64 {
	if r == nil {
		return 0
	}
	return r.dropped.Load()
}

// Close stops dispatch, drains observer channels, and rejects further
// Register calls.
func (r *Runtime) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for id, obs := range r.observers {
		delete(r.observers, id)
		close(obs.ch)
	}
}

func (r *registered) run() {
	for ev := range r.ch {
		r.observer.OnApply(ev)
	}
}

// Close removes this registration from its runtime and closes the
// dispatch channel. Safe to call concurrently and idempotent.
func (r *registered) Close() {
	if r == nil || r.runtime == nil {
		return
	}
	r.once.Do(func() {
		r.runtime.mu.Lock()
		defer r.runtime.mu.Unlock()
		if current := r.runtime.observers[r.id]; current == r {
			delete(r.runtime.observers, r.id)
			close(r.ch)
		}
	})
}

// String implements fmt.Stringer for Source so logs and tests get a
// readable form.
func (s Source) String() string {
	switch s {
	case SourceCommit:
		return "commit"
	case SourceResolveLock:
		return "resolve-lock"
	default:
		return fmt.Sprintf("source(%d)", uint8(s))
	}
}
