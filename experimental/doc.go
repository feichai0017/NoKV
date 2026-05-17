// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package experimental marks research mechanisms that are not part of NoKV's
// stable product contract.
//
// Experimental packages may depend on stable fsmeta, engine, txn, raftstore,
// coordinator, and root interfaces. Stable packages must not import
// experimental packages.
package experimental
