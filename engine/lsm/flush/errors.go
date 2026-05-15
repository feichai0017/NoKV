// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package flush

import "errors"

// Sentinel errors returned by Runtime.Enqueue.
var (
	ErrNil    = errors.New("flush: runtime is nil")
	ErrClosed = errors.New("flush: runtime closed")
)
