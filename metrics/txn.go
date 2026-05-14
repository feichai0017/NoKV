// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

// TxnMetrics captures MVCC transaction activity counters.
type TxnMetrics struct {
	Started   uint64
	Committed uint64
	Conflicts uint64
	Active    int64
}
