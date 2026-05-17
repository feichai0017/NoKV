// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	errRunnerRequired                = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec: runner required")
	errInodeAllocatorRequired        = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec: inode allocator required")
	errAuditorRunnerRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec: auditor runner required")
	errSubtreeHandoffWithoutFrontier = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/exec: subtree handoff started without committed frontier")
	errVisibleAuthorityNotHeld       = nokverrors.New(nokverrors.KindNotLeader, "fsmeta/exec: visible authority is held by another holder")
	errVisibleOverlayFallbackUnsafe  = nokverrors.New(nokverrors.KindUnavailable, "fsmeta/exec: visible overlay read cannot safely fall back to base mutation")

	ErrVisibleAdmissionRejected   = nokverrors.New(nokverrors.KindConflict, "fsmeta/exec: visible admission rejected")
	ErrVisibleIneligibleOperation = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec: operation is not eligible for visible commit")
)
