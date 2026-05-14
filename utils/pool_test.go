// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"expvar"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPoolSubmitAndSize(t *testing.T) {
	pool := NewPool(0, "test")
	done := make(chan struct{})
	require.NoError(t, pool.Submit(func() {
		close(done)
	}))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pool task did not complete")
	}

	require.Equal(t, 1, pool.Size())
	require.Nil(t, expvar.Get("NoKV.Pool.test.Submit"))
	require.Nil(t, expvar.Get("NoKV.Pool.test.Active"))
	require.NoError(t, pool.Submit(nil))
	pool.Release()
}
