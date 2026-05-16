// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
)

var (
	ErrClientRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority client required")
	ErrTableRequired      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority table required")
	ErrHolderRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: holder id required")
	ErrTTLInvalid         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority ttl must be non-negative")
	ErrInvalidResponse    = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/peras: invalid authority response")
	ErrNotHeld            = nokverrors.New(nokverrors.KindNotLeader, "fsmeta/runtime/peras: authority is held by another holder")
	ErrRuntimeInvalid     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: runtime config is invalid")
	ErrRuntimeClosed      = nokverrors.New(nokverrors.KindUnavailable, "fsmeta/runtime/peras: runtime is closed")
	ErrPublishRequired    = nokverrors.New(nokverrors.KindStaleEpoch, "fsmeta/runtime/peras: published flush requires active publish authority")
	ErrInvalidGrant       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: invalid authority grant")
	ErrAmbiguousAuthority = nokverrors.New(nokverrors.KindConflict, "fsmeta/runtime/peras: ambiguous active authority")
	ErrConflictingGrant   = nokverrors.New(nokverrors.KindConflict, "fsmeta/runtime/peras: conflicting authority grant")
	ErrAuthorityViewStale = nokverrors.New(nokverrors.KindStaleEpoch, "fsmeta/runtime/peras: active authority view stale")
	ErrVisibleLogClosed   = nokverrors.New(nokverrors.KindUnavailable, "fsmeta/runtime/peras: visible log is closed")
)

func IsNotHeld(err error) bool {
	return errors.Is(err, ErrNotHeld)
}

func isAdmissionTerminalError(err error) bool {
	return errors.Is(err, fsmeta.ErrExists) ||
		errors.Is(err, fsmeta.ErrNotFound) ||
		errors.Is(err, fsmeta.ErrInvalidRequest) ||
		errors.Is(err, fsmeta.ErrInvalidValue)
}
