// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package router

import "errors"

var (
	// ErrRegisterNilPeer indicates Register received a nil peer.
	ErrRegisterNilPeer = errors.New("raftstore/router: cannot register nil peer")
	// ErrNilCommandRequest indicates SendCommand received a nil request.
	ErrNilCommandRequest = errors.New("raftstore/router: nil raft command request")
)
