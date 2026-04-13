package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorHelpers(t *testing.T) {
	err := errors.New("boom")
	require.PanicsWithValue(t, err, func() { Panic(err) })
	require.NotPanics(t, func() { Panic(nil) })

	require.PanicsWithValue(t, err, func() { CondPanic(true, err) })
	require.NotPanics(t, func() { CondPanic(false, err) })
	require.Panics(t, func() {
		CondPanicFunc(true, func() error { return err })
	})
	require.NotPanics(t, func() {
		CondPanicFunc(false, func() error { return err })
	})
}
