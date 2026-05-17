// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package proof defines the versioned proof records carried by metadata
// programs after visible admission.
//
// The package owns stable schema ids, rule ids, digest construction, and local
// verifiers for predicate and guard evidence. These records are holder/frontier
// evidence used by visible admission, segment completion, and replay validation;
// they are not storage-engine cryptographic range proofs.
package proof
