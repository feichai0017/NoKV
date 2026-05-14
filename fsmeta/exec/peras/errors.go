// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrInvalidPerasSegment             = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/exec/peras: invalid peras segment")
	ErrAdmissionRejected               = nokverrors.New(nokverrors.KindConflict, "fsmeta/exec/peras: admission rejected")
	ErrHolderConfigInvalid             = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec/peras: invalid holder config")
	ErrIneligibleOperation             = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/exec/peras: ineligible operation")
	ErrInvalidOperationID              = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec/peras: invalid operation id")
	ErrDuplicateOperation              = nokverrors.New(nokverrors.KindConflict, "fsmeta/exec/peras: duplicate operation id")
	ErrSegmentCatalogStoreRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec/peras: segment catalog store required")
	ErrReplayVersionRequired           = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/exec/peras: replay version required")
	ErrInvalidWitnessRecord            = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/exec/peras: invalid witness record")
	ErrWitnessLogRequired              = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec/peras: witness log required")
	ErrVisibleLogRequired              = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec/peras: visible log required")
	ErrWitnessReplicaInvalid           = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/exec/peras: invalid witness replica")
	ErrSegmentWitnessQuorumUnavailable = nokverrors.New(nokverrors.KindUnavailable, "fsmeta/exec/peras: segment witness quorum unavailable")
)
