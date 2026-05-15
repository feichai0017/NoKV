// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package proof

import "errors"

// ErrInvalidProof is returned by predicate and guard proof verifiers when an
// envelope, scope digest, or evidence binding fails the Version1 schema.
var ErrInvalidProof = errors.New("fsmeta/proof: invalid proof")
