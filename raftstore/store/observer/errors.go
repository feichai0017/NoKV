// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package observer

import "errors"

// Sentinel errors returned by Runtime.Register.
var (
	ErrNilObserver = errors.New("raftstore/observer: nil observer")
	ErrClosed      = errors.New("raftstore/observer: runtime closed")
)
