// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mmap

// Advice encodes high-level madvise hints. Values mirror x/sys/unix flags
// where available; callers should prefer these symbolic constants over raw
// integers.
type Advice int

const (
	AdviceNormal Advice = iota
	AdviceSequential
	AdviceRandom
	AdviceWillNeed
	AdviceDontNeed
)
