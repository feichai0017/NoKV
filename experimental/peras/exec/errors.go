// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrInvalidPerasSegment             = nokverrors.New(nokverrors.KindProtocolViolation, "experimental/peras/exec: invalid peras segment")
	ErrAdmissionRejected               = nokverrors.New(nokverrors.KindConflict, "experimental/peras/exec: admission rejected")
	ErrHolderConfigInvalid             = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/exec: invalid holder config")
	ErrIneligibleOperation             = nokverrors.New(nokverrors.KindProtocolViolation, "experimental/peras/exec: ineligible operation")
	ErrInvalidOperationID              = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/exec: invalid operation id")
	ErrDuplicateOperation              = nokverrors.New(nokverrors.KindConflict, "experimental/peras/exec: duplicate operation id")
	ErrSegmentCatalogStoreRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/exec: segment catalog store required")
	ErrReplayVersionRequired           = nokverrors.New(nokverrors.KindProtocolViolation, "experimental/peras/exec: replay version required")
	ErrInvalidWitnessRecord            = nokverrors.New(nokverrors.KindProtocolViolation, "experimental/peras/exec: invalid witness record")
	ErrWitnessLogRequired              = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/exec: witness log required")
	ErrVisibleLogRequired              = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/exec: visible log required")
	ErrWitnessReplicaInvalid           = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/exec: invalid witness replica")
	ErrSegmentWitnessQuorumUnavailable = nokverrors.New(nokverrors.KindUnavailable, "experimental/peras/exec: segment witness quorum unavailable")
)
