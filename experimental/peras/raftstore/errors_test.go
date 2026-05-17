// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestRaftstorePerasErrorsCarryStableKinds(t *testing.T) {
	tests := []struct {
		name string
		err  error
		kind nokverrors.Kind
	}{
		{name: "witness config", err: ErrWitnessNodeConfigInvalid, kind: nokverrors.KindInvalidArgument},
		{name: "missing authority", err: ErrWitnessAuthorityMissing, kind: nokverrors.KindStaleEpoch},
		{name: "authority mismatch", err: ErrWitnessAuthorityMismatch, kind: nokverrors.KindStaleEpoch},
		{name: "install request", err: ErrInvalidInstallRequest, kind: nokverrors.KindProtocolViolation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.kind, nokverrors.KindOf(tt.err))
			require.Equal(t, tt.kind, nokverrors.KindOf(fmt.Errorf("wrapped: %w", tt.err)))
		})
	}
}

func TestWitnessAuthorityLagIsRetryable(t *testing.T) {
	require.True(t, nokverrors.Retryable(ErrWitnessAuthorityMissing))
	require.True(t, nokverrors.Retryable(ErrWitnessAuthorityMismatch))
}
