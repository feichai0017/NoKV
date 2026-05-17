// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrWitnessNodeConfigInvalid = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/raftstore: invalid witness node config")
	ErrWitnessAuthorityMissing  = nokverrors.New(nokverrors.KindStaleEpoch, "experimental/peras/raftstore: missing active authority")
	ErrWitnessAuthorityMismatch = nokverrors.New(nokverrors.KindStaleEpoch, "experimental/peras/raftstore: authority mismatch")
	ErrInvalidInstallRequest    = nokverrors.New(nokverrors.KindProtocolViolation, "experimental/peras/raftstore: invalid install request")
)
