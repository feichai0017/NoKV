//go:build darwin || linux

package mmap

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestAdviseFromBool(t *testing.T) {
	require.Equal(t, AdviceNormal, adviseFromBool(true))
	require.Equal(t, AdviceRandom, adviseFromBool(false))
}

func TestMsyncAsyncRangeRejectsInvalid(t *testing.T) {
	err := MsyncAsyncRange([]byte{}, 0, 0)
	require.ErrorIs(t, err, unix.EINVAL)
}
