// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// Runtime is a distributed fsmeta runtime backed by the Rust MetadataPlane.
type Runtime struct {
	Runner   *Runner
	Executor *fsmetaexec.Executor
	Routes   *CoordinatorRouteProvider
	Mounts   *MountResolver
	Inodes   *InodeAllocator
	Watcher  *Watcher
	Snapshot *SnapshotPublisher

	once sync.Once
}

func Open(ctx context.Context, opts Options) (*Runtime, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	routes, err := NewCoordinatorRouteProvider(opts.Coordinator, CoordinatorRouteProviderOptions{
		DialOptions: opts.DialOptions,
		DialTimeout: opts.DialTimeout,
	})
	if err != nil {
		return nil, err
	}
	tso, err := NewCoordinatorTimestampSource(opts.Coordinator)
	if err != nil {
		_ = routes.Close()
		return nil, err
	}
	runner, err := NewRunner(routes, tso)
	if err != nil {
		_ = routes.Close()
		return nil, err
	}
	mounts, err := NewMountResolver(opts.Coordinator)
	if err != nil {
		_ = routes.Close()
		return nil, err
	}
	inodes, err := NewInodeAllocator(opts.Coordinator)
	if err != nil {
		_ = routes.Close()
		return nil, err
	}
	watcher, err := NewWatcher(routes, mounts)
	if err != nil {
		_ = routes.Close()
		return nil, err
	}
	snapshot, err := NewSnapshotPublisher(opts.Coordinator)
	if err != nil {
		_ = routes.Close()
		return nil, err
	}
	execOpts := []fsmetaexec.Option{
		fsmetaexec.WithMountResolver(mounts),
		fsmetaexec.WithSubtreeAuthorityResolver(mounts),
		fsmetaexec.WithSubtreeHandoffPublisher(mounts),
		fsmetaexec.WithInodeAllocator(inodes),
	}
	if opts.LockTTL > 0 {
		execOpts = append(execOpts, fsmetaexec.WithLockTTL(uint64((opts.LockTTL+time.Millisecond-1)/time.Millisecond)))
	}
	if opts.Clock != nil {
		execOpts = append(execOpts, fsmetaexec.WithClock(opts.Clock))
	}
	executor, err := fsmetaexec.New(runner, execOpts...)
	if err != nil {
		_ = routes.Close()
		return nil, err
	}
	if opts.BootstrapMount != "" {
		mount, err := mounts.ResolveMount(ctx, opts.BootstrapMount)
		if err != nil {
			_ = routes.Close()
			return nil, err
		}
		if err := BootstrapRootInode(ctx, runner, mount, opts.Clock); err != nil {
			_ = routes.Close()
			return nil, err
		}
	}
	return &Runtime{
		Runner:   runner,
		Executor: executor,
		Routes:   routes,
		Mounts:   mounts,
		Inodes:   inodes,
		Watcher:  watcher,
		Snapshot: snapshot,
	}, nil
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		if r.Routes != nil {
			err = r.Routes.Close()
		}
	})
	return err
}

func BootstrapRootInode(ctx context.Context, runner *Runner, mount fsmetaexec.MountAdmission, now func() time.Time) error {
	key, err := layout.EncodeInodeKey(mount.Identity(), mount.RootInode)
	if err != nil {
		return err
	}
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	if err != nil {
		return err
	}
	if _, ok, err := runner.Get(ctx, key, readVersion); err != nil || ok {
		return err
	}
	ts := time.Now()
	if now != nil {
		ts = now()
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
	startVersion, err := runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return err
	}
	_, err = runner.CommitMetadata(ctx, backend.MetadataCommand{
		PrimaryKey:    key,
		ReadVersion:   startVersion,
		CommitVersion: startVersion + 1,
		WatchKeys:     [][]byte{key},
		Mutations: []*backend.Mutation{{
			Op:                backend.MutationPut,
			Key:               key,
			Value:             value,
			AssertionNotExist: true,
		}},
	})
	return err
}
