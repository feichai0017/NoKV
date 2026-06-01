// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

// Options configures the embedded fsmeta runtime.
type Options struct {
	// DB reuses an already-open Badger database. When nil, Open creates and
	// owns a Badger DB under WorkDir.
	DB *badger.DB

	// DBOptions configures a runtime-owned Badger DB. When nil, Open uses
	// Badger defaults with SyncWrites disabled for the local demo path.
	DBOptions *badger.Options

	// WorkDir is required when DB is nil.
	WorkDir string

	// Mount is the single local fsmeta mount admitted by this runtime.
	Mount model.MountIdentity

	// RootInode is the mount root inode. Zero uses model.RootInode.
	RootInode model.InodeID

	// LockTTL overrides fsmeta/exec's lock TTL, in the same units expected by
	// fsmeta/exec.WithLockTTL. Local MVCC commits are one-phase, but keeping the
	// option lets callers share runtime configuration.
	LockTTL time.Duration

	// Clock overrides fsmeta/exec's wall clock for write-session expiry.
	Clock func() time.Time
}

func (opts Options) rootInode() model.InodeID {
	if opts.RootInode != 0 {
		return opts.RootInode
	}
	return model.RootInode
}

func (opts Options) validate() error {
	if opts.Mount.MountID == "" || opts.Mount.MountKeyID == 0 {
		return errMountRequired
	}
	if opts.DB == nil && opts.WorkDir == "" {
		return errWorkDirRequired
	}
	return nil
}

func localDBOptions(opts Options) badger.Options {
	dir := localBadgerDir(opts.WorkDir)
	cfg := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(false)
	if opts.DBOptions != nil {
		cfg = *opts.DBOptions
		if cfg.Dir == "" {
			cfg.Dir = dir
		}
		if cfg.ValueDir == "" {
			cfg.ValueDir = cfg.Dir
		}
	}
	return cfg
}
