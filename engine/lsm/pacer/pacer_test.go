// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package pacer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPacerChargesByConfiguredRate(t *testing.T) {
	now := time.Unix(0, 0)
	var slept time.Duration
	p := New(100)
	p.SetClock(
		func() time.Time { return now },
		func(d time.Duration) {
			slept += d
			now = now.Add(d)
		},
	)

	p.Charge(250)

	stats := p.Stats()
	require.Equal(t, int64(250), stats.BytesCharged)
	require.Equal(t, 2500*time.Millisecond, slept)
	require.Equal(t, slept.Nanoseconds(), stats.NanosThrottle)
}
