// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// InodeAllocator assigns monotonically increasing local inode IDs.
type InodeAllocator struct {
	mount model.MountIdentity

	mu   sync.Mutex
	next uint64

	total       atomic.Uint64
	affineHit   atomic.Uint64
	affineProbe atomic.Uint64
}

// NewInodeAllocator initializes allocation above every inode key already
// present for mount.
func NewInodeAllocator(runner *Runner, mount model.MountIdentity) (*InodeAllocator, error) {
	maxInode, err := maxInodeInStore(runner, mount)
	if err != nil {
		return nil, err
	}
	if maxInode < model.RootInode {
		maxInode = model.RootInode
	}
	alloc := &InodeAllocator{mount: mount}
	alloc.next = uint64(maxInode) + 1
	return alloc, nil
}

// AllocateCreateInode implements fsmetaexec.InodeAllocator.
func (a *InodeAllocator) AllocateCreateInode(_ context.Context, mount model.MountIdentity, parent model.InodeID, name string) (model.InodeID, error) {
	if a == nil {
		return 0, errMountRequired
	}
	if mount != a.mount {
		return 0, model.ErrMountNotRegistered
	}
	target, err := localCreateDentryBucket(mount, parent, name)
	if err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		id := model.InodeID(a.next)
		a.next++
		if id <= model.RootInode {
			continue
		}
		a.affineProbe.Add(1)
		if layout.BucketForInodeID(id) != target {
			continue
		}
		a.total.Add(1)
		a.affineHit.Add(1)
		return id, nil
	}
}

// Stats returns local allocator diagnostics.
func (a *InodeAllocator) Stats() map[string]any {
	if a == nil {
		return map[string]any{
			"next_inode":                       uint64(0),
			"inode_alloc_total":                uint64(0),
			"inode_alloc_affinity_hit_total":   uint64(0),
			"inode_alloc_affinity_probe_total": uint64(0),
		}
	}
	a.mu.Lock()
	next := a.next
	a.mu.Unlock()
	return map[string]any{
		"next_inode":                       next,
		"inode_alloc_total":                a.total.Load(),
		"inode_alloc_affinity_hit_total":   a.affineHit.Load(),
		"inode_alloc_affinity_probe_total": a.affineProbe.Load(),
	}
}

func localCreateDentryBucket(mount model.MountIdentity, parent model.InodeID, name string) (layout.AffinityBucket, error) {
	if parent == model.RootInode {
		return layout.ChooseWorkspaceBucket(mount, name), nil
	}
	key, err := layout.EncodeDentryKey(mount, parent, name)
	if err != nil {
		return 0, err
	}
	bucket, ok := layout.BucketOfKey(key)
	if !ok {
		return 0, layout.ErrInvalidKey
	}
	return bucket, nil
}

func maxInodeInStore(runner *Runner, mount model.MountIdentity) (model.InodeID, error) {
	if runner == nil {
		return 0, nil
	}
	prefix, err := layout.EncodeMountPrefix(mount)
	if err != nil {
		return 0, err
	}
	var maxInode model.InodeID
	err = runner.scanUserKeys(prefix, prefix, func(userKey []byte) (bool, error) {
		parts, ok := layout.InspectKey(userKey)
		if ok && parts.MountKeyID == mount.MountKeyID && parts.Kind == layout.KeyKindInode && parts.Inode > maxInode {
			maxInode = parts.Inode
		}
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	return maxInode, nil
}
