// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"google.golang.org/grpc"
)

type Options struct {
	Coordinator CoordinatorClient
	DialOptions []grpc.DialOption
	DialTimeout time.Duration
	// BootstrapMount idempotently creates the mount root inode if missing.
	// Empty means the caller only wants to open the runtime client.
	BootstrapMount model.MountID
	LockTTL        time.Duration
	Clock          func() time.Time
}

func (opts Options) validate() error {
	if opts.Coordinator == nil {
		return errCoordinatorRequired
	}
	return nil
}
