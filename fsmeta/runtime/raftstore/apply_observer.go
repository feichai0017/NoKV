// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

// ApplyObserver adapts raftstore apply notifications into fsmeta watch events.
// The router itself stays storage-runtime neutral.
type ApplyObserver struct {
	router *fsmetawatch.Router
}

func NewApplyObserver(router *fsmetawatch.Router) *ApplyObserver {
	return &ApplyObserver{router: router}
}

func (o *ApplyObserver) OnApply(evt storepkg.ApplyEvent) {
	if o == nil || o.router == nil {
		return
	}
	source := fsmeta.WatchEventSource(0)
	switch evt.Source {
	case storepkg.ApplyEventSourceCommit:
		source = fsmeta.WatchEventSourceCommit
	case storepkg.ApplyEventSourceResolveLock:
		source = fsmeta.WatchEventSourceResolveLock
	default:
		return
	}
	o.router.OnApply(fsmeta.ApplyEvent{
		RegionID:      evt.RegionID,
		Term:          evt.Term,
		Index:         evt.Index,
		Source:        source,
		CommitVersion: evt.CommitVersion,
		Keys:          evt.Keys,
	})
}
