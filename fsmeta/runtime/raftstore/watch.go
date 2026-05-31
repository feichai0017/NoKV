// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	metadatapb "github.com/feichai0017/NoKV/pb/metadata"
)

const defaultWatchBuffer = 256

// Watcher adapts Rust MetadataPlane apply notifications into fsmeta watch
// streams.
type Watcher struct {
	routes RouteProvider
	mounts fsmetaexec.MountResolver

	subscriptions atomic.Uint64
	events        atomic.Uint64
	dropped       atomic.Uint64
	expired       atomic.Uint64
	overflow      atomic.Uint64
}

func NewWatcher(routes RouteProvider, mounts fsmetaexec.MountResolver) (*Watcher, error) {
	if routes == nil {
		return nil, errRouteProviderRequired
	}
	if mounts == nil {
		return nil, errCoordinatorRequired
	}
	return &Watcher{routes: routes, mounts: mounts}, nil
}

func (w *Watcher) Subscribe(ctx context.Context, req observe.WatchRequest) (observe.WatchSubscription, error) {
	if w == nil || w.routes == nil {
		return nil, errRouteProviderRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	prefix, err := w.watchPrefix(ctx, req)
	if err != nil {
		return nil, err
	}
	route, err := w.routes.RouteForKey(ctx, prefix)
	if err != nil {
		return nil, err
	}
	buffer := req.BackPressureWindow
	if buffer == 0 {
		buffer = defaultWatchBuffer
	}
	subCtx, cancel := context.WithCancel(ctx)
	stream, err := route.Client.WatchApply(subCtx, &metadatapb.MetadataWatchApplyRequest{
		KeyPrefix:      cloneBytes(prefix),
		Buffer:         buffer,
		ResumeRegionId: req.ResumeCursor.RegionID,
		ResumeTerm:     req.ResumeCursor.Term,
		ResumeIndex:    req.ResumeCursor.Index,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	sub := &metadataWatchSubscription{
		events: make(chan observe.WatchEvent, buffer),
		cancel: cancel,
		ready:  req.ResumeCursor,
	}
	w.subscriptions.Add(1)
	go w.pump(sub, stream)
	return sub, nil
}

func (w *Watcher) Stats() map[string]any {
	if w == nil {
		return map[string]any{
			"subscriptions_total": uint64(0),
			"events_total":        uint64(0),
			"dropped_total":       uint64(0),
			"expired_total":       uint64(0),
			"overflow_total":      uint64(0),
		}
	}
	return map[string]any{
		"subscriptions_total": w.subscriptions.Load(),
		"events_total":        w.events.Load(),
		"dropped_total":       w.dropped.Load(),
		"expired_total":       w.expired.Load(),
		"overflow_total":      w.overflow.Load(),
	}
}

func (w *Watcher) watchPrefix(ctx context.Context, req observe.WatchRequest) ([]byte, error) {
	if len(req.KeyPrefix) > 0 {
		return observe.WatchPrefix(req)
	}
	record, err := w.mounts.ResolveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	return observe.WatchPrefixForMount(req, record.Identity())
}

func (w *Watcher) pump(sub *metadataWatchSubscription, stream metadatapb.MetadataPlane_WatchApplyClient) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				sub.finish(nil)
				return
			}
			sub.finish(err)
			return
		}
		if dropped := resp.GetDroppedEvents(); dropped > 0 {
			w.dropped.Add(dropped)
			w.expired.Add(1)
			sub.finish(model.ErrWatchCursorExpired)
			return
		}
		evt := resp.GetEvent()
		if evt == nil {
			continue
		}
		source := metadataWatchSource(evt.GetSource())
		if source == 0 {
			continue
		}
		cursor := observe.WatchCursor{
			RegionID: evt.GetRegionId(),
			Term:     evt.GetTerm(),
			Index:    evt.GetIndex(),
		}
		for _, key := range evt.GetKeys() {
			if len(key) == 0 {
				continue
			}
			watchEvent := observe.WatchEvent{
				Cursor:        cursor,
				CommitVersion: evt.GetCommitVersion(),
				Source:        source,
				Key:           cloneBytes(key),
			}
			select {
			case sub.events <- watchEvent:
				w.events.Add(1)
			default:
				w.dropped.Add(1)
				w.overflow.Add(1)
				sub.finish(model.ErrWatchOverflow)
				return
			}
		}
	}
}

func metadataWatchSource(source metadatapb.MetadataApplyWatchEventSource) observe.WatchEventSource {
	switch source {
	case metadatapb.MetadataApplyWatchEventSource_METADATA_APPLY_WATCH_EVENT_SOURCE_COMMIT:
		return observe.WatchEventSourceCommit
	default:
		return 0
	}
}

type metadataWatchSubscription struct {
	events chan observe.WatchEvent
	cancel context.CancelFunc
	ready  observe.WatchCursor
	once   sync.Once

	mu  sync.Mutex
	err error
}

func (s *metadataWatchSubscription) Events() <-chan observe.WatchEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *metadataWatchSubscription) ReadyCursor() observe.WatchCursor {
	if s == nil {
		return observe.WatchCursor{}
	}
	return s.ready
}

func (s *metadataWatchSubscription) Ack(observe.WatchCursor) {}

func (s *metadataWatchSubscription) Close() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
}

func (s *metadataWatchSubscription) Err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *metadataWatchSubscription) finish(err error) {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.events)
	})
}
