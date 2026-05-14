// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"errors"
	"testing"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/stretchr/testify/require"
)

func TestProposalBatcherRejectsAfterClose(t *testing.T) {
	p := newTestPeer(t, newPayloadTestStorage(), nil)
	require.NoError(t, p.Close())

	done := make(chan error, 1)
	go func() {
		done <- p.Propose([]byte("after-close"))
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, errPeerStopped)
	case <-time.After(time.Second):
		t.Fatal("proposal after close blocked")
	}
}

func TestProposalBatcherCompletesDroppedProposalOnce(t *testing.T) {
	storage := myraft.NewMemoryStorage()
	p, err := NewPeer(&Config{
		RaftConfig: myraft.Config{
			ID:                        11,
			ElectionTick:              5,
			HeartbeatTick:             1,
			MaxSizePerMsg:             1 << 20,
			MaxInflightMsgs:           256,
			MaxUncommittedEntriesSize: 1,
			PreVote:                   true,
		},
		Transport:    noopPayloadTransport{},
		Apply:        func([]myraft.Entry) error { return nil },
		Storage:      storage,
		GroupID:      1,
		BatchMaxSize: 1,
		BatchMaxWait: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	result := p.batcher.propose([]byte("larger-than-one-byte"))
	select {
	case err := <-result.Done:
		require.Error(t, err)
		require.False(t, errors.Is(err, errPeerStopped))
	case <-time.After(time.Second):
		t.Fatal("dropped proposal did not complete")
	}

	select {
	case err := <-result.Done:
		t.Fatalf("proposal completed more than once: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
}
