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

func TestThrottleDoDoneError(t *testing.T) {
	th := NewThrottle(1)
	require.NoError(t, th.Do())
	th.Done(errors.New("boom"))
	require.Error(t, th.Finish())
}
