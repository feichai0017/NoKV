<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Error Handling

NoKV keeps semantic errors near the package that owns the condition.

## Rules

- Use typed sentinel errors for public semantic states such as not found,
  already exists, stale epoch, invalid request, or root grant mismatch.
- Do not string-match errors.
- Wrap with `%w` when adding context.
- Convert to protobuf/gRPC errors only at server/client boundaries.
- Keep lower semantic packages independent from transport error details.

## fsmeta

`fsmeta/model` owns namespace-level errors. `fsmeta/exec` preserves those
errors while compiling and executing operations. `fsmeta/server` converts them
to the public wire form.

## Root and Coordinator

`meta/root` owns rooted truth errors. `coordinator` may classify and return
serving-plane errors, but it must not invent a conflicting rooted state.

## raftstore

Rust data-plane errors should be converted at the protobuf service boundary.
OpenRaft and Holt implementation details should not leak into Go fsmeta domain
types.
