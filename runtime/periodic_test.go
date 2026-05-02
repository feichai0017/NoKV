package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPeriodicTaskRunsAndRecordsErrors(t *testing.T) {
	var runs atomic.Uint64
	task := NewPeriodicTask(PeriodicTaskConfig{
		Name:     "test",
		Interval: time.Millisecond,
		Run: func(context.Context) error {
			runs.Add(1)
			return errors.New("boom")
		},
	})
	require.NotNil(t, task)
	task.Start()

	require.Eventually(t, func() bool {
		snap := task.Snapshot()
		return snap.Enabled && snap.Runs > 0 && snap.LastError == "boom"
	}, time.Second, 10*time.Millisecond)
	require.Greater(t, runs.Load(), uint64(0))
	task.Close()
	task.Close()
}

func TestPeriodicTaskDisabledWithoutNameIntervalOrRun(t *testing.T) {
	require.Nil(t, NewPeriodicTask(PeriodicTaskConfig{}))
	require.Nil(t, NewPeriodicTask(PeriodicTaskConfig{Name: "x"}))
	require.Nil(t, NewPeriodicTask(PeriodicTaskConfig{Name: "x", Interval: time.Millisecond}))
}

func TestPeriodicTaskCloseBeforeStartDoesNotBlock(t *testing.T) {
	task := NewPeriodicTask(PeriodicTaskConfig{
		Name:     "x",
		Interval: time.Hour,
		Run:      func(context.Context) error { return nil },
	})
	require.NotNil(t, task)

	done := make(chan struct{})
	go func() {
		task.Close()
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}
