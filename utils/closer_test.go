package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCloserCloseSignalsAndWaits(t *testing.T) {
	closer := NewCloserInitial(1)
	done := make(chan struct{})
	go func() {
		<-closer.Closed()
		close(done)
		closer.Done()
	}()

	closer.Close()
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
		<-closer.Closed()
		closer.Done()
	}()
	closer.Close()
	require.True(t, true)
}

func TestCloserCloseIsIdempotent(t *testing.T) {
	closer := NewCloser()
	closer.Close()
	closer.Close()
	require.True(t, true)
}
