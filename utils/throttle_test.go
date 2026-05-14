// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"errors"
	"testing"

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
