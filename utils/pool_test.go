package utils

import (
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
	require.NotNil(t, GetOrCreateInt("NoKV.Pool.test.Submit"))
	require.NoError(t, pool.Submit(nil))
	pool.Release()
}
