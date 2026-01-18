package main

import (
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/stretchr/testify/require"
)

func TestMainListenFailure(t *testing.T) {
	withMainArgs(t, []string{"nokv-redis", "-workdir", t.TempDir(), "-addr", "127.0.0.1:0"}, func() {
		origListen := listen
		defer func() { listen = origListen }()
		listen = func(network, address string) (net.Listener, error) {
			return nil, errors.New("listen fail")
		}
		code := captureExitCode(t, func() {
			main()
		})
		require.Equal(t, 1, code)
	})
}

func TestMainRaftConfigFailure(t *testing.T) {
	withMainArgs(t, []string{"nokv-redis", "-raft-config", "missing.json"}, func() {
		code := captureExitCode(t, func() {
			main()
		})
		require.Equal(t, 1, code)
	})
}

func TestMainServeErrorBranch(t *testing.T) {
	withMainArgs(t, []string{"nokv-redis", "-workdir", t.TempDir(), "-addr", "127.0.0.1:0"}, func() {
		origListen := listen
		defer func() { listen = origListen }()
		listen = func(network, address string) (net.Listener, error) {
			return &stubListener{err: errors.New("accept fail")}, nil
		}
		origExit := exit
		defer func() { exit = origExit }()
		exit = func(code int) {
			panic(code)
		}
		main()
	})
}

func TestMainSignalBranch(t *testing.T) {
	withMainArgs(t, []string{
		"nokv-redis",
		"-workdir", t.TempDir(),
		"-addr", "127.0.0.1:0",
		"-metrics-addr", "bad",
	}, func() {
		origNotify := signalNotify
		defer func() { signalNotify = origNotify }()
		signalNotify = func(ch chan<- os.Signal, _ ...os.Signal) {
			ch <- os.Interrupt
		}

		origOptions := newDefaultOptions
		defer func() { newDefaultOptions = origOptions }()
		newDefaultOptions = func() *NoKV.Options {
			opt := NoKV.NewDefaultOptions()
			opt.MaxBatchCount = 0
			opt.MaxBatchSize = 0
			opt.WriteBatchMaxCount = 0
			opt.WriteBatchMaxSize = 0
			return opt
		}

		origExit := exit
		defer func() { exit = origExit }()
		exit = func(code int) {
			panic(code)
		}

		done := make(chan struct{})
		go func() {
			main()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for main to exit")
		}
	})
}

type stubListener struct {
	err error
}

func (l *stubListener) Accept() (net.Conn, error) { return nil, l.err }
func (l *stubListener) Close() error              { return nil }
func (l *stubListener) Addr() net.Addr            { return &net.TCPAddr{} }

func withMainArgs(t *testing.T, args []string, fn func()) {
	t.Helper()
	origArgs := os.Args
	origFlags := flag.CommandLine
	flag.CommandLine = flag.NewFlagSet(args[0], flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = args
	defer func() {
		os.Args = origArgs
		flag.CommandLine = origFlags
	}()
	fn()
}

func captureExitCode(t *testing.T, fn func()) (code int) {
	t.Helper()
	origExit := exit
	defer func() { exit = origExit }()
	exit = func(code int) {
		panic(code)
	}
	defer func() {
		if r := recover(); r != nil {
			if c, ok := r.(int); ok {
				code = c
				return
			}
			panic(r)
		}
	}()
	fn()
	return code
}
