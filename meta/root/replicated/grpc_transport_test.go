// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestGRPCTransportSendsToPeer(t *testing.T) {
	t1, err := NewGRPCTransport(1, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t1.Close() })

	t2, err := NewGRPCTransport(2, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t2.Close() })

	t1.SetPeer(2, t2.Addr())

	var (
		mu   sync.Mutex
		got  []raftpb.Message
		done = make(chan struct{}, 1)
	)
	t2.SetHandler(func(msg raftpb.Message) error {
		mu.Lock()
		got = append(got, msg)
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})

	err = t1.Send(raftpb.Message{From: 1, To: 2, Type: raftpb.MsgHeartbeat, Term: 3, Index: 9})
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for grpc transport delivery")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1)
	require.Equal(t, uint64(1), got[0].From)
	require.Equal(t, uint64(2), got[0].To)
	require.Equal(t, raftpb.MsgHeartbeat, got[0].Type)
	require.Equal(t, uint64(3), got[0].Term)
	require.Equal(t, uint64(9), got[0].Index)
}

func TestGRPCTransportRejectsUnknownPeer(t *testing.T) {
	t1, err := NewGRPCTransport(1, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t1.Close() })

	err = t1.Send(raftpb.Message{From: 1, To: 2, Type: raftpb.MsgHeartbeat})
	require.Error(t, err)
}

func TestGRPCTransportUnreachableDialDoesNotBlockLivePeer(t *testing.T) {
	deadAddr, deadAccepted := startBlackholeListener(t)

	t1, err := NewGRPCTransport(1, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t1.Close() })
	t1.dialTimeout = 1200 * time.Millisecond

	t3, err := NewGRPCTransport(3, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = t3.Close() })

	t1.SetPeer(2, deadAddr)
	t1.SetPeer(3, t3.Addr())

	deadDone := make(chan error, 1)
	go func() {
		_, err := t1.clientFor(2)
		deadDone <- err
	}()

	select {
	case <-deadAccepted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unreachable peer dial to start")
	}

	sendDone := make(chan error, 1)
	start := time.Now()
	go func() {
		sendDone <- t1.Send(raftpb.Message{From: 1, To: 3, Type: raftpb.MsgHeartbeat, Term: 7})
	}()

	select {
	case err := <-sendDone:
		require.NoError(t, err)
		require.Less(t, time.Since(start), 300*time.Millisecond)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("live peer send was blocked behind unreachable peer dial")
	}

	select {
	case err := <-deadDone:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for unreachable peer dial to fail")
	}
}

func startBlackholeListener(t *testing.T) (string, <-chan struct{}) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	accepted := make(chan struct{}, 1)
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- struct{}{}:
			default:
			}
			go func() {
				defer func() { _ = conn.Close() }()
				<-stop
			}()
		}
	}()

	t.Cleanup(func() {
		close(stop)
		_ = ln.Close()
		<-done
	})

	return ln.Addr().String(), accepted
}
