// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import (
	"context"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"sync"
)

// virtualLogAdapter owns the rooted virtual-log view that sits beneath the
// replicated protocol driver. Callers must hold the enclosing driver mutex
// before invoking the mutating helpers.
type virtualLogAdapter struct {
	mu         sync.Mutex
	log        rootstorage.VirtualLog
	notifyCh   chan struct{}
	latest     rootstorage.TailToken
	metrics    metrics
	cacheValid bool
	cache      rootstorage.ObservedCommitted
}

func newVirtualLogAdapter(log rootstorage.VirtualLog) (*virtualLogAdapter, error) {
	adapter := &virtualLogAdapter{
		log:      log,
		notifyCh: make(chan struct{}, 1),
	}
	if err := adapter.bootstrap(); err != nil {
		return nil, err
	}
	return adapter, nil
}

func (a *virtualLogAdapter) bootstrap() error {
	if a == nil || a.log == nil {
		return nil
	}
	observed, err := a.loadObservedLocked(0)
	if err != nil {
		return err
	}
	a.latest.Cursor = observed.LastCursor()
	if a.latest.Cursor != (rootstate.Cursor{}) || observed.Checkpoint.TailOffset != 0 || len(observed.Tail.Records) > 0 {
		a.latest.Revision = 1
	}
	return nil
}

func (a *virtualLogAdapter) watchChannel() <-chan struct{} {
	if a == nil {
		return nil
	}
	return a.notifyCh
}

func (a *virtualLogAdapter) observe(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	observed, err := a.observeCommittedLocked()
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	a.latest.Cursor = observed.LastCursor()
	return observed.Advance(after, a.latest), nil
}

func (a *virtualLogAdapter) closedAdvance(after rootstorage.TailToken) rootstorage.TailAdvance {
	if a == nil {
		return rootstorage.ObservedCommitted{}.Advance(after, rootstorage.TailToken{})
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return rootstorage.ObservedCommitted{}.Advance(after, a.latest)
}

func (a *virtualLogAdapter) installBootstrap(observed rootstorage.ObservedCommitted) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.log.InstallBootstrap(observed); err != nil {
		return err
	}
	a.cache = rootstorage.CloneObservedCommitted(observed)
	a.cacheValid = true
	a.bump(observed.LastCursor())
	a.signal()
	return nil
}

func (a *virtualLogAdapter) appendCommitted(ctx context.Context, records []rootstorage.CommittedEvent) error {
	if a == nil || len(records) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.log.AppendCommitted(ctx, records...); err != nil {
		return err
	}
	a.invalidateCacheLocked()
	a.bump(records[len(records)-1].Cursor)
	a.signal()
	return nil
}

func (a *virtualLogAdapter) observeCommitted() (rootstorage.ObservedCommitted, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.observeCommittedLocked()
}

func (a *virtualLogAdapter) observeCommittedLocked() (rootstorage.ObservedCommitted, error) {
	if a.cacheValid {
		a.metrics.observeCacheHitTotal.Add(1)
		return rootstorage.CloneObservedCommitted(a.cache), nil
	}
	a.metrics.observeCacheMissTotal.Add(1)
	observed, err := a.loadObservedLocked(0)
	if err != nil {
		return rootstorage.ObservedCommitted{}, err
	}
	a.cache = rootstorage.CloneObservedCommitted(observed)
	a.cacheValid = true
	return rootstorage.CloneObservedCommitted(observed), nil
}

// loadObservedLocked is the only checkpoint+tail read behind the observe
// cache. All local commit, checkpoint, compaction, and bootstrap publishers
// invalidate or replace the cache before exposing a newer tail token.
func (a *virtualLogAdapter) loadObservedLocked(offset int64) (rootstorage.ObservedCommitted, error) {
	a.metrics.checkpointLoadTotal.Add(1)
	checkpoint, err := a.log.LoadCheckpoint()
	if err != nil {
		return rootstorage.ObservedCommitted{}, err
	}
	a.metrics.committedTailReadTotal.Add(1)
	tail, err := a.log.ReadCommitted(offset)
	if err != nil {
		return rootstorage.ObservedCommitted{}, err
	}
	return rootstorage.ObservedCommitted{Checkpoint: checkpoint, Tail: tail}, nil
}

func (a *virtualLogAdapter) invalidateCacheLocked() {
	if a == nil {
		return
	}
	if a.cacheValid {
		a.metrics.observeCacheInvalidations.Add(1)
	}
	a.cacheValid = false
	a.cache = rootstorage.ObservedCommitted{}
}

func (a *virtualLogAdapter) loadCheckpoint() (rootstorage.Checkpoint, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.log.LoadCheckpoint()
}

func (a *virtualLogAdapter) saveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.log.SaveCheckpoint(checkpoint); err != nil {
		return err
	}
	a.invalidateCacheLocked()
	a.bump(checkpoint.Snapshot.State.LastCommitted)
	a.signal()
	return nil
}

func (a *virtualLogAdapter) readCommitted(offset int64) (rootstorage.CommittedTail, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.log.ReadCommitted(offset)
}

func (a *virtualLogAdapter) compactCommitted(stream rootstorage.CommittedTail) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.log.CompactCommitted(stream); err != nil {
		return err
	}
	a.invalidateCacheLocked()
	a.bump(stream.TailCursor(a.latest.Cursor))
	a.signal()
	return nil
}

func (a *virtualLogAdapter) size() (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.log.Size()
}

func (a *virtualLogAdapter) stats() map[string]any {
	if a == nil {
		return map[string]any{}
	}
	return a.metrics.snapshot()
}

func (a *virtualLogAdapter) bump(cursor rootstate.Cursor) {
	a.latest.Cursor = cursor
	a.latest.Revision++
}

func (a *virtualLogAdapter) signal() {
	select {
	case a.notifyCh <- struct{}{}:
	default:
	}
}
