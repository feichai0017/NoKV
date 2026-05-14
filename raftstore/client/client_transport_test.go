// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitRetryUsesLockResolveBackoff(t *testing.T) {
	cli := &Client{
		retry: RetryPolicy{
			MaxAttempts:        2,
			LockResolveBackoff: time.Hour,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := cli.waitRetry(ctx, 0, retryLockResolve)
	require.ErrorIs(t, err, context.Canceled)
}
