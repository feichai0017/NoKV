// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package peras is the planned home for Peras visible-before-durable metadata
// execution experiments.
//
// The current Peras implementation still lives under fsmeta/runtime/peras,
// fsmeta/exec/peras, and raftstore/peras while the migration is staged. New
// Peras-only code should move here once its stable storage interfaces are in
// place.
package peras
