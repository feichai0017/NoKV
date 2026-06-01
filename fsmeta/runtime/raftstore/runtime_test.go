// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestRetryBootstrapRootInodeRetriesRouteRefresh(t *testing.T) {
	attempts := 0
	err := retryBootstrapRootInodeWithBackoff(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return nokverrors.New(nokverrors.KindStaleEpoch, "coordinator root view is catching up")
		}
		return nil
	}, time.Second, 0, 0)

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryBootstrapRootInodeRetriesNotLeader(t *testing.T) {
	attempts := 0
	err := retryBootstrapRootInodeWithBackoff(context.Background(), func(context.Context) error {
		attempts++
		if attempts == 1 {
			return nokverrors.New(nokverrors.KindNotLeader, "metadata root moved")
		}
		return nil
	}, time.Second, 0, 0)

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

func TestRetryBootstrapRootInodeDoesNotRetrySemanticError(t *testing.T) {
	attempts := 0
	want := nokverrors.New(nokverrors.KindInvalidArgument, "bad bootstrap mount")
	err := retryBootstrapRootInodeWithBackoff(context.Background(), func(context.Context) error {
		attempts++
		return want
	}, time.Second, 0, 0)

	require.ErrorIs(t, err, want)
	require.Equal(t, 1, attempts)
}
