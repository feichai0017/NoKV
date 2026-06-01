// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"maps"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
)

const (
	localWatchRegionID uint64 = 1
	localWatchTerm     uint64 = 1
)

// Watcher turns local MVCC commits into fsmeta watch events.
type Watcher struct {
	*fsmetawatch.Router
	mounts fsmetaexec.MountResolver
	next   atomic.Uint64
}

// NewWatcher constructs a local watch router.
func NewWatcher(mounts fsmetaexec.MountResolver) *Watcher {
	return &Watcher{
		Router: fsmetawatch.NewRouter(),
		mounts: mounts,
	}
}

// Subscribe implements observe.Watcher with local mount admission.
func (w *Watcher) Subscribe(ctx context.Context, req observe.WatchRequest) (observe.WatchSubscription, error) {
	if w == nil || w.Router == nil {
		return nil, model.ErrInvalidRequest
	}
	if req.Mount != "" && w.mounts != nil {
		record, err := w.mounts.ResolveMount(ctx, req.Mount)
		if err != nil {
			return nil, err
		}
		if record.MountID == "" {
			return nil, model.ErrMountNotRegistered
		}
		if record.Retired {
			return nil, model.ErrMountRetired
		}
		prefix, err := observe.WatchPrefixForMount(req, record.Identity())
		if err != nil {
			return nil, err
		}
		req.KeyPrefix = prefix
	}
	return w.Router.Subscribe(ctx, req)
}

// ObserveMetadataCommand publishes one replayable local watch cursor after a
// metadata command has been durably applied.
func (w *Watcher) ObserveMetadataCommand(commitVersion uint64, command backend.MetadataCommand) {
	if w == nil || w.Router == nil || commitVersion == 0 {
		return
	}
	events := commandWatchEvents(command)
	if len(events) == 0 {
		return
	}
	cursor := observe.WatchCursor{
		RegionID: localWatchRegionID,
		Term:     localWatchTerm,
		Index:    w.next.Add(1),
	}
	for _, event := range events {
		w.Publish(observe.WatchEvent{
			Cursor:        cursor,
			CommitVersion: commitVersion,
			Source:        observe.WatchEventSourceCommit,
			Key:           cloneBytes(event.Key),
			Namespace:     watchEventNamespace(event),
		})
	}
}

// Stats returns local watch diagnostics.
func (w *Watcher) Stats() map[string]any {
	if w == nil || w.Router == nil {
		return map[string]any{
			"subscribers":     0,
			"regions":         0,
			"recent_events":   0,
			"events_total":    uint64(0),
			"delivered_total": uint64(0),
			"dropped_total":   uint64(0),
			"overflow_total":  uint64(0),
			"next_cursor":     uint64(0),
		}
	}
	out := copyStats(w.Router.Stats())
	out["next_cursor"] = w.next.Load()
	return out
}

func mutationWatchKeys(mutations []*backend.Mutation) [][]byte {
	seen := map[string]struct{}{}
	keys := make([][]byte, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation == nil {
			continue
		}
		switch mutation.Op {
		case backend.MutationPut, backend.MutationDelete:
		default:
			continue
		}
		key := mutation.Key
		if len(key) == 0 {
			continue
		}
		id := string(key)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		keys = append(keys, cloneBytes(key))
	}
	return keys
}

func commandWatchEvents(command backend.MetadataCommand) []backend.WatchEvent {
	if len(command.WatchEvents) != 0 {
		out := make([]backend.WatchEvent, 0, len(command.WatchEvents))
		for _, event := range command.WatchEvents {
			if len(event.Key) == 0 {
				continue
			}
			event.Key = cloneBytes(event.Key)
			out = append(out, event)
		}
		return out
	}
	keys := mutationWatchKeys(command.Mutations)
	out := make([]backend.WatchEvent, 0, len(keys))
	for _, key := range keys {
		out = append(out, backend.WatchEvent{Key: key})
	}
	return out
}

func watchEventNamespace(event backend.WatchEvent) observe.NamespaceEvent {
	return observe.NamespaceEvent{
		Operation: observeWatchOperation(event.Operation),
		Parent:    model.InodeID(event.Parent),
		Name:      event.Name,
		Inode:     model.InodeID(event.Inode),
		OldParent: model.InodeID(event.OldParent),
		OldName:   event.OldName,
		NewParent: model.InodeID(event.NewParent),
		NewName:   event.NewName,
	}
}

func observeWatchOperation(op backend.WatchOperation) observe.WatchOperation {
	switch op {
	case backend.WatchOperationCreate:
		return observe.WatchOperationCreate
	case backend.WatchOperationUpdate:
		return observe.WatchOperationUpdate
	case backend.WatchOperationDelete:
		return observe.WatchOperationDelete
	case backend.WatchOperationRename:
		return observe.WatchOperationRename
	case backend.WatchOperationReplace:
		return observe.WatchOperationReplace
	case backend.WatchOperationLink:
		return observe.WatchOperationLink
	default:
		return observe.WatchOperationUnspecified
	}
}

func copyStats(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	maps.Copy(out, src)
	return out
}
