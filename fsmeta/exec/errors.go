// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	errRunnerRequired                = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec: runner required")
	errInodeAllocatorRequired        = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec: inode allocator required")
	errAuditorRunnerRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec: auditor runner required")
	errSubtreeHandoffWithoutFrontier = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/exec: subtree handoff started without committed frontier")
)
