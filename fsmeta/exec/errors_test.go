// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestExecErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errRunnerRequired))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errAuditorRunnerRequired))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errSubtreeHandoffWithoutFrontier))
}
