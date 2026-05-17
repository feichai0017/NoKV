// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrWitnessNodeConfigInvalid = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/adapters/raftstore: invalid witness node config")
	ErrWitnessAuthorityMissing  = nokverrors.New(nokverrors.KindStaleEpoch, "experimental/peras/adapters/raftstore: missing active authority")
	ErrWitnessAuthorityMismatch = nokverrors.New(nokverrors.KindStaleEpoch, "experimental/peras/adapters/raftstore: authority mismatch")
	ErrInvalidInstallRequest    = nokverrors.New(nokverrors.KindProtocolViolation, "experimental/peras/adapters/raftstore: invalid install request")
)
