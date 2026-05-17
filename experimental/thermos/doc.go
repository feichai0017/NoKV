// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package thermos contains the optional hotspot-detection experiment.
//
// Stable engine paths should depend on narrow observation interfaces. Direct
// imports from local write-admission code are migration-era exceptions tracked
// by the import-boundary tests.
package thermos
