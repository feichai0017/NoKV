// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package raftstore adapts fsmeta execution to the Rust metadata data plane.
//
// The package owns no namespace semantics and no rooted topology truth. It
// translates fsmeta/backend commands to MetadataPlane RPCs, consumes a
// rebuildable route view, and relies on coordinator/root outside this package
// to supply fresh region context and timestamps.
package raftstore
