// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"
	"time"

	execperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestOpenRejectsInvalidSessionCleanupLimit(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr:     "127.0.0.1:1",
		SessionCleanupLimit: fsmeta.MaxSessionExpireLimit + 1,
	})
	require.ErrorContains(t, err, "session cleanup limit exceeds maximum")
}

func TestOpenRejectsNegativeLockTTL(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr: "127.0.0.1:1",
		LockTTL:         -time.Millisecond,
	})
	require.ErrorIs(t, err, errLockTTLInvalid)
}

func TestOpenRejectsNegativePerasAuthorityTTL(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr:   "127.0.0.1:1",
		PerasAuthorityTTL: -time.Millisecond,
	})
	require.ErrorIs(t, err, runtimeperas.ErrTTLInvalid)
}

func TestOpenRejectsNegativePerasSegmentMutationBudget(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr:                "127.0.0.1:1",
		PerasSegmentMaxReplayMutations: -1,
	})
	require.ErrorIs(t, err, runtimeperas.ErrRuntimeInvalid)
}

func TestOpenRejectsNegativePerasSegmentInstallParallelism(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr:                "127.0.0.1:1",
		PerasSegmentInstallParallelism: -1,
	})
	require.ErrorIs(t, err, runtimeperas.ErrRuntimeInvalid)
}

func TestOpenRejectsNegativePerasSegmentFlushParallelism(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr:              "127.0.0.1:1",
		PerasSegmentFlushParallelism: -1,
	})
	require.ErrorIs(t, err, runtimeperas.ErrRuntimeInvalid)
}

func TestOpenRejectsNegativePerasSegmentCatalogRouteBudget(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr:                "127.0.0.1:1",
		PerasSegmentCatalogRouteBudget: -1,
	})
	require.ErrorIs(t, err, runtimeperas.ErrRuntimeInvalid)
}

func TestOpenRejectsPerasWithoutVisibleLog(t *testing.T) {
	_, err := Open(context.Background(), Options{
		CoordinatorAddr: "127.0.0.1:1",
		PerasHolderID:   "holder-a",
	})
	require.ErrorIs(t, err, execperas.ErrVisibleLogRequired)
}
