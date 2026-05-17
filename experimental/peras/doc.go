// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package peras marks Peras visible-before-durable metadata execution as an
// explicit experiment.
//
// The implementation is split below this package by the stable boundary it
// currently adapts to. Stable packages may only import those subpackages from
// explicit Peras adapter files while the migration is in progress.
package peras
