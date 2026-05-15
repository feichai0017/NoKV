// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import "errors"

// ErrWorkloadFailed is returned by a workload runner when at least one
// generated operation hit a non-retryable failure. Callers wrap it with the
// per-driver operation count and a small sample of failure summaries.
var ErrWorkloadFailed = errors.New("benchmark/fsmeta/workload: workload completed with operation errors")
