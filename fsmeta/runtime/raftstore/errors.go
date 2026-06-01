// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	errClientRequired          = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: metadata client is required")
	errRouteProviderRequired   = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: route provider is required")
	errTimestampSourceRequired = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: timestamp source is required")
	errInvalidMetadataCommand  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: invalid metadata command")
	errCoordinatorRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: coordinator client is required")
	errBackendRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: metadata backend is required")
)
