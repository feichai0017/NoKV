// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

func (r *Runner) recordMutationMetrics(atomic bool) {
	if atomic {
		r.atomicMutateTotal.Add(1)
	}
	r.mutateTotal.Add(1)
}

func (r *Runner) recordAtomicMutationMetric() {
	r.atomicMutateTotal.Add(1)
}
