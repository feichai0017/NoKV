package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCloserSignalAndWait(t *testing.T) {
	closer := NewCloserInitial(1)
	done := make(chan struct{})
	go func() {
		<-closer.HasBeenClosed()
		close(done)
		closer.Done()
	}()

	closer.SignalAndWait()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("closer did not signal")
	}
}

func TestCloserClose(t *testing.T) {
	closer := NewCloser()
	closer.Add(1)
	go func() {
		<-closer.CloseSignal
		closer.Done()
	}()
	closer.Close()
	require.True(t, true)
}
