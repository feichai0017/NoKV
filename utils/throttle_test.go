// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThrottleGoAndFinish(t *testing.T) {
	th := NewThrottle(1)
	require.NoError(t, th.Go(func() error { return nil }))
	require.NoError(t, th.Finish())
}

func TestThrottleGoError(t *testing.T) {
	th := NewThrottle(1)
	require.NoError(t, th.Go(func() error { return errors.New("boom") }))
	require.Error(t, th.Finish())
}

func TestThrottleFinishDoesNotBlockWhenErrorsExceedWorkers(t *testing.T) {
	th := NewThrottle(2)
	for range 8 {
		require.NoError(t, th.Go(func() error { return errors.New("boom") }))
	}
	done := make(chan error, 1)
	go func() { done <- th.Finish() }()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("Throttle.Finish blocked after more tasks failed than worker slots")
	}
}
