// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package idalloc

import "errors"

// ErrInvalidBatch indicates a requested allocation batch is invalid.
var ErrInvalidBatch = errors.New("coordinator/idalloc: invalid batch")
