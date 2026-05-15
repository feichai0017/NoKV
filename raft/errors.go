// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raft

import etcdraft "go.etcd.io/raft/v3"

// Sentinel aliases for etcd/raft errors. Callers consume these so they do not
// import the upstream package directly. The aliases preserve identity so
// errors.Is against etcd/raft sentinels remains correct.
var (
	ErrCompacted                      = etcdraft.ErrCompacted
	ErrSnapOutOfDate                  = etcdraft.ErrSnapOutOfDate
	ErrUnavailable                    = etcdraft.ErrUnavailable
	ErrSnapshotTemporarilyUnavailable = etcdraft.ErrSnapshotTemporarilyUnavailable
	ErrStop                           = etcdraft.ErrStopped
)
