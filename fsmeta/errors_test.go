// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestErrorsExposeStableKinds(t *testing.T) {
	invalid := []error{
		ErrInvalidMountID,
		ErrInvalidInodeID,
		ErrInvalidName,
		ErrInvalidSession,
		ErrInvalidRequest,
		ErrInvalidKey,
		ErrInvalidKeyKind,
		ErrInvalidValue,
		ErrInvalidValueKind,
		ErrInvalidPageSize,
	}
	for _, err := range invalid {
		require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(err))
		require.True(t, nokverrors.IsKind(fmt.Errorf("wrapped: %w", err), nokverrors.KindInvalidArgument))
	}

	require.Equal(t, nokverrors.KindAlreadyExists, nokverrors.KindOf(ErrExists))
	require.Equal(t, nokverrors.KindNotFound, nokverrors.KindOf(ErrNotFound))
	require.Equal(t, nokverrors.KindNotFound, nokverrors.KindOf(ErrMountNotRegistered))
	require.Equal(t, nokverrors.KindAborted, nokverrors.KindOf(ErrMountRetired))
	require.Equal(t, nokverrors.KindResourceExhausted, nokverrors.KindOf(ErrQuotaExceeded))
	require.Equal(t, nokverrors.KindResourceExhausted, nokverrors.KindOf(ErrWatchOverflow))
	require.Equal(t, nokverrors.KindStaleEpoch, nokverrors.KindOf(ErrWatchCursorExpired))
}
