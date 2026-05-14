// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	stderrors "errors"
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestRootStateErrorsExposeStableKinds(t *testing.T) {
	tests := []struct {
		err  error
		kind nokverrors.Kind
	}{
		{rootstate.ErrPrimacy, nokverrors.KindConflict},
		{rootstate.ErrInvalidGrant, nokverrors.KindInvalidArgument},
		{rootstate.ErrInheritance, nokverrors.KindProtocolViolation},
		{rootstate.ErrDuty, nokverrors.KindProtocolViolation},
		{rootstate.ErrSilence, nokverrors.KindProtocolViolation},
		{rootstate.ErrFinality, nokverrors.KindConflict},
	}
	for _, tt := range tests {
		wrapped := fmt.Errorf("wrapped: %w", tt.err)
		require.Equal(t, tt.kind, nokverrors.KindOf(tt.err))
		require.True(t, nokverrors.IsKind(wrapped, tt.kind))
		require.True(t, stderrors.Is(wrapped, tt.err))
	}
}
