// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
)

// SnapshotRegistry tracks locally published snapshot tokens. Local fsmeta does
// not have rooted truth or snapshot-driven GC yet, so retirement is idempotent:
// a restarted server can retire a valid token without requiring old in-memory
// state to still exist.
type SnapshotRegistry struct {
	mu     sync.Mutex
	active map[localSnapshotKey]struct{}

	publishTotal atomic.Uint64
	retireTotal  atomic.Uint64
}

type localSnapshotKey struct {
	mount       fsmeta.MountID
	mountKeyID  fsmeta.MountKeyID
	rootInode   fsmeta.InodeID
	readVersion uint64
}

// NewSnapshotRegistry constructs an empty local snapshot registry.
func NewSnapshotRegistry() *SnapshotRegistry {
	return &SnapshotRegistry{active: make(map[localSnapshotKey]struct{})}
}

// PublishSnapshotSubtree records a local snapshot token.
func (r *SnapshotRegistry) PublishSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	if err := validateSnapshotToken(token); err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.active == nil {
		r.active = make(map[localSnapshotKey]struct{})
	}
	r.active[localSnapshotKeyFromToken(token)] = struct{}{}
	r.mu.Unlock()
	r.publishTotal.Add(1)
	return nil
}

// RetireSnapshotSubtree removes a local snapshot token when it is still known.
func (r *SnapshotRegistry) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	if err := validateSnapshotToken(token); err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	r.mu.Lock()
	delete(r.active, localSnapshotKeyFromToken(token))
	r.mu.Unlock()
	r.retireTotal.Add(1)
	return nil
}

// Stats returns local snapshot registry diagnostics.
func (r *SnapshotRegistry) Stats() map[string]any {
	if r == nil {
		return map[string]any{
			"active_snapshots":     0,
			"publish_total":        uint64(0),
			"retire_total":         uint64(0),
			"durability_authority": "local_mvcc",
		}
	}
	r.mu.Lock()
	active := len(r.active)
	r.mu.Unlock()
	return map[string]any{
		"active_snapshots":     active,
		"publish_total":        r.publishTotal.Load(),
		"retire_total":         r.retireTotal.Load(),
		"durability_authority": "local_mvcc",
	}
}

func validateSnapshotToken(token fsmeta.SnapshotSubtreeToken) error {
	if token.Mount == "" || token.MountKeyID == 0 || token.RootInode == 0 || token.ReadVersion == 0 {
		return fsmeta.ErrInvalidRequest
	}
	for _, ref := range token.PerasSegmentRefs {
		if !ref.Valid() {
			return fsmeta.ErrInvalidRequest
		}
	}
	return nil
}

func localSnapshotKeyFromToken(token fsmeta.SnapshotSubtreeToken) localSnapshotKey {
	return localSnapshotKey{
		mount:       token.Mount,
		mountKeyID:  token.MountKeyID,
		rootInode:   token.RootInode,
		readVersion: token.ReadVersion,
	}
}
