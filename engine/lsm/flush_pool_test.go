package lsm

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

// stubMemIndex is the minimum memIndex impl flushPool tests need: it only
// has to track ref-count calls so IncrRef / DecrRef on memTable wrappers
// are no-ops.
type stubMemIndex struct {
	refs atomic.Int64
}

func (s *stubMemIndex) Add(*kv.Entry)                          {}
func (s *stubMemIndex) Search([]byte) ([]byte, kv.ValueStruct) { return nil, kv.ValueStruct{} }
func (s *stubMemIndex) NewIterator(*index.Options) index.Iterator {
	return nil
}
func (s *stubMemIndex) MemSize() int64 { return 0 }
func (s *stubMemIndex) IncrRef()       { s.refs.Add(1) }
func (s *stubMemIndex) DecrRef()       { s.refs.Add(-1) }

func newStubMemTable(shard *lsmShard, segID uint32) *memTable {
	mt := &memTable{
		shard:     shard,
		segmentID: segID,
		index:     &stubMemIndex{},
	}
	shard.immutables = append(shard.immutables, mt)
	return mt
}

func TestFlushPoolSubmitDrainsAndInvokesInstall(t *testing.T) {
	var (
		mu        sync.Mutex
		installed []uint32
	)
	install := func(mt *memTable) error {
		mu.Lock()
		installed = append(installed, mt.segmentID)
		mu.Unlock()
		return nil
	}

	p := newFlushPool(1, install)
	p.Start(1)

	shard := &lsmShard{id: 0}
	mt := newStubMemTable(shard, 7)

	require.NoError(t, p.Submit(mt))
	require.Eventually(t, func() bool { return p.Pending() == 0 },
		time.Second, time.Millisecond)
	require.NoError(t, p.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []uint32{7}, installed)

	// Worker pool removed the memtable from the shard's immutables list.
	shard.lock.RLock()
	defer shard.lock.RUnlock()
	require.Empty(t, shard.immutables)
}

func TestFlushPoolInstallErrorLeavesMemtableInImmutables(t *testing.T) {
	install := func(*memTable) error { return errors.New("synthetic install failure") }

	p := newFlushPool(1, install)
	p.Start(1)

	shard := &lsmShard{id: 0}
	mt := newStubMemTable(shard, 11)
	require.NoError(t, p.Submit(mt))

	require.Eventually(t, func() bool { return p.Pending() == 0 },
		time.Second, time.Millisecond)
	require.NoError(t, p.Close())

	// install failure: pool must NOT have removed the memtable from the
	// shard so a retry / restart can still observe it.
	shard.lock.RLock()
	defer shard.lock.RUnlock()
	require.Len(t, shard.immutables, 1)
	require.Equal(t, mt, shard.immutables[0])
}

func TestFlushPoolNilSubmitAndNilShardGuards(t *testing.T) {
	p := newFlushPool(1, func(*memTable) error { return nil })
	p.Start(1)
	defer func() { _ = p.Close() }()

	require.NoError(t, p.Submit(nil), "nil memtable submit should be a no-op")

	mt := &memTable{index: &stubMemIndex{}}
	require.ErrorIs(t, p.Submit(mt), ErrFlushNilMemtable)
}

func TestFlushPoolStatsReflectQueueAndCompletion(t *testing.T) {
	release := make(chan struct{})
	install := func(*memTable) error {
		<-release
		return nil
	}

	p := newFlushPool(1, install)
	p.Start(1)

	shard := &lsmShard{id: 0}
	mt := newStubMemTable(shard, 1)
	require.NoError(t, p.Submit(mt))

	require.Eventually(t, func() bool { return p.Pending() == 1 },
		time.Second, time.Millisecond)

	close(release)
	require.Eventually(t, func() bool { return p.Pending() == 0 },
		time.Second, time.Millisecond)
	require.NoError(t, p.Close())

	require.Equal(t, int64(1), p.Stats().Completed)
}

func TestFlushPoolNilReceiverGuards(t *testing.T) {
	var p *flushPool
	require.Equal(t, int64(0), p.Pending())
	require.NotPanics(t, func() { _ = p.Close() })
}
