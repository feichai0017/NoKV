package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorHelpers(t *testing.T) {
	err := errors.New("boom")
	require.NotNil(t, Err(err))
	require.NotNil(t, WarpErr("wrap", err))
	require.Nil(t, Err(nil))
	require.Nil(t, WarpErr("wrap", nil))

	require.PanicsWithValue(t, err, func() { Panic(err) })
	require.NotPanics(t, func() { Panic(nil) })
	require.PanicsWithValue(t, err, func() { Panic2(nil, err) })

	require.PanicsWithValue(t, err, func() { CondPanic(true, err) })
	require.NotPanics(t, func() { CondPanic(false, err) })
	require.Panics(t, func() {
		CondPanicFunc(true, func() error { return err })
	})
	require.NotPanics(t, func() {
		CondPanicFunc(false, func() error { return err })
	})
}

func TestWrapHelpers(t *testing.T) {
	err := errors.New("boom")
	require.Nil(t, Wrap(nil, "msg"))
	require.Contains(t, Wrap(err, "msg").Error(), "msg")
	require.Nil(t, Wrapf(nil, "msg %d", 1))
	require.Contains(t, Wrapf(err, "msg %d", 2).Error(), "msg 2")
	require.NotEmpty(t, location(1, false))
}
