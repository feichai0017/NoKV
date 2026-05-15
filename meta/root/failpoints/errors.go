// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package failpoints

import "errors"

// Sentinel errors surfaced when the matching rooted control-plane failpoint
// fires. Tests assert on these via errors.Is rather than message strings.
var (
	ErrBeforeApplyGrantIssue                = errors.New("meta/root failpoint: before apply coordinator grant")
	ErrBeforeApplyGrantRetirement           = errors.New("meta/root failpoint: before apply coordinator grant retirement")
	ErrBeforeGrantStorageRead               = errors.New("meta/root failpoint: before coordinator grant storage read")
	ErrAfterAppendCommittedBeforeCheckpoint = errors.New("meta/root failpoint: after append committed before checkpoint")
)
