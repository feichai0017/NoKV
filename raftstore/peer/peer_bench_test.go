// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
)

type slowStorage struct {
	raftlog.PeerStorage
	appendDelay time.Duration
}

func (s *slowStorage) Append(entries []myraft.Entry) error {
	time.Sleep(s.appendDelay)
	return s.PeerStorage.Append(entries)
}

func (s *slowStorage) AppendWithHardState(entries []myraft.Entry, st myraft.HardState) error {
	time.Sleep(s.appendDelay)
	if aws, ok := s.PeerStorage.(raftlog.AppendWithHardStateStorage); ok {
		return aws.AppendWithHardState(entries, st)
	}
	if err := s.PeerStorage.Append(entries); err != nil {
		return err
	}
	return s.SetHardState(st)
}

func newBenchPeer(b *testing.B, batchSize int, batchWait time.Duration, fsyncDelay time.Duration) *Peer {
	b.Helper()
	memStore := myraft.NewMemoryStorage()
	storage := raftlog.PeerStorage(memStore)
	if fsyncDelay > 0 {
		storage = &slowStorage{PeerStorage: storage, appendDelay: fsyncDelay}
	}
	apply := func([]myraft.Entry) error { return nil }
	cfg := &Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
			Storage:         memStore,
		},
		Transport:    noopPayloadTransport{},
		Apply:        apply,
		Storage:      storage,
		GroupID:      1,
		BatchMaxSize: batchSize,
		BatchMaxWait: batchWait,
	}
	p, err := NewPeer(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Close() })
	if err := p.Bootstrap([]myraft.Peer{{ID: 1}}); err != nil {
		b.Fatal(err)
	}
	if err := p.Campaign(); err != nil {
		b.Fatal(err)
	}
	if err := p.Flush(); err != nil {
		b.Fatal(err)
	}
	return p
}

func benchmarkPeerPropose(b *testing.B, batchSize int, batchWait time.Duration, fsyncDelay time.Duration, concurrency int) {
	p := newBenchPeer(b, batchSize, batchWait, fsyncDelay)
	payload := []byte("bench-proposal-data-32-bytes--")

	total := b.N
	perGoroutine := total / concurrency
	extra := total % concurrency

	var count atomic.Int64
	var wg sync.WaitGroup
	wg.Add(concurrency)

	b.ResetTimer()
	for g := range concurrency {
		n := perGoroutine
		if g < extra {
			n++
		}
		go func(n int) {
			defer wg.Done()
			for range n {
				if err := p.Propose(payload); err != nil {
					b.Error(err)
					return
				}
				count.Add(1)
			}
		}(n)
	}
	wg.Wait()
	b.StopTimer()

	if int(count.Load()) != total {
		b.Errorf("proposed %d, expected %d", count.Load(), total)
	}
	qps := float64(count.Load()) / b.Elapsed().Seconds()
	_, _ = fmt.Fprintf(b.Output(), "  %.0f proposals/sec\n", qps)
}

func BenchmarkPeerPropose(b *testing.B) {
	b.Run("batch64_1ms/conc1", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, 0, 1) })
	b.Run("batch64_1ms/conc8", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, 0, 8) })
	b.Run("batch64_1ms/conc64", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, 0, 64) })
	b.Run("batch64_1ms/conc256", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, 0, 256) })
	b.Run("batch256_1ms/conc8", func(b *testing.B) { benchmarkPeerPropose(b, 256, time.Millisecond, 0, 8) })
	b.Run("batch256_1ms/conc64", func(b *testing.B) { benchmarkPeerPropose(b, 256, time.Millisecond, 0, 64) })
	b.Run("batch256_1ms/conc256", func(b *testing.B) { benchmarkPeerPropose(b, 256, time.Millisecond, 0, 256) })
}

func BenchmarkPeerProposeFsync(b *testing.B) {
	b.Run("batch64_1ms/conc1", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, time.Millisecond, 1) })
	b.Run("batch64_1ms/conc8", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, time.Millisecond, 8) })
	b.Run("batch64_1ms/conc64", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, time.Millisecond, 64) })
	b.Run("batch64_1ms/conc256", func(b *testing.B) { benchmarkPeerPropose(b, 64, time.Millisecond, time.Millisecond, 256) })
	b.Run("batch256_1ms/conc8", func(b *testing.B) { benchmarkPeerPropose(b, 256, time.Millisecond, time.Millisecond, 8) })
	b.Run("batch256_1ms/conc64", func(b *testing.B) { benchmarkPeerPropose(b, 256, time.Millisecond, time.Millisecond, 64) })
	b.Run("batch256_1ms/conc256", func(b *testing.B) { benchmarkPeerPropose(b, 256, time.Millisecond, time.Millisecond, 256) })
}
