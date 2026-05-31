// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type mountRootInitializer interface {
	EnsureMountRoot(context.Context, fsmetaexec.MountAdmission) error
}

type rootBootstrapper struct {
	runner backend.Store
	now    func() time.Time

	mu   sync.Mutex
	done map[rootBootstrapKey]struct{}
}

type rootBootstrapKey struct {
	mountID    model.MountID
	mountKeyID model.MountKeyID
	rootInode  model.InodeID
}

func newRootBootstrapper(runner backend.Store, now func() time.Time) *rootBootstrapper {
	if runner == nil {
		return nil
	}
	return &rootBootstrapper{runner: runner, now: now}
}

func (b *rootBootstrapper) EnsureMountRoot(ctx context.Context, mount fsmetaexec.MountAdmission) error {
	if b == nil || b.runner == nil || mount.Retired {
		return nil
	}
	key := rootBootstrapKey{
		mountID:    mount.MountID,
		mountKeyID: mount.MountKeyID,
		rootInode:  mount.RootInode,
	}
	if key.mountID == "" || key.mountKeyID == 0 || key.rootInode == 0 {
		return model.ErrMountNotRegistered
	}
	if b.isDone(key) {
		return nil
	}
	if err := b.bootstrap(ctx, mount); err != nil {
		return err
	}
	b.markDone(key)
	return nil
}

func (b *rootBootstrapper) bootstrap(ctx context.Context, mount fsmetaexec.MountAdmission) error {
	storageKey, err := layout.EncodeInodeKey(mount.Identity(), mount.RootInode)
	if err != nil {
		return err
	}
	readVersion, err := b.runner.ReserveTimestamp(ctx, 1)
	if err != nil {
		return err
	}
	if _, ok, err := b.runner.Get(ctx, storageKey, readVersion); err != nil || ok {
		return err
	}
	ts := time.Now()
	if b.now != nil {
		ts = b.now()
	}
	value, err := layout.EncodeInodeValue(model.InodeRecord{
		Inode:         mount.RootInode,
		Type:          model.InodeTypeDirectory,
		LinkCount:     1,
		CreatedUnixNs: ts.UnixNano(),
		UpdatedUnixNs: ts.UnixNano(),
	})
	if err != nil {
		return err
	}
	startVersion, err := b.runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return err
	}
	_, err = b.runner.Mutate(ctx, storageKey, []*backend.Mutation{{
		Op:                backend.MutationPut,
		Key:               storageKey,
		Value:             value,
		AssertionNotExist: true,
	}}, startVersion, startVersion+1, 0)
	if nokverrors.IsKind(err, nokverrors.KindAlreadyExists) {
		return nil
	}
	return err
}

func (b *rootBootstrapper) isDone(key rootBootstrapKey) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.done[key]
	return ok
}

func (b *rootBootstrapper) markDone(key rootBootstrapKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done == nil {
		b.done = make(map[rootBootstrapKey]struct{})
	}
	b.done[key] = struct{}{}
}
