package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

func TestRequestDecrRefUnderflowPanics(t *testing.T) {
	req := &Request{}
	require.Panics(t, func() {
		req.DecrRef()
	})
}

func TestRequestResetAndLoadEntries(t *testing.T) {
	oldEntry := kv.NewEntry([]byte("old"), []byte("value"))
	req := &Request{
		Entries:    []*kv.Entry{oldEntry},
		Ptrs:       []kv.ValuePtr{{Len: 1}},
		PtrIdxs:    []int{1},
		PtrBuckets: []uint32{7},
		Err:        errors.New("boom"),
		EnqueueAt:  time.Now(),
	}
	req.RefCount.Init(3)
	req.WG.Add(1)

	newEntries := []*kv.Entry{
		kv.NewEntry([]byte("k1"), []byte("v1")),
		kv.NewEntry([]byte("k2"), []byte("v2")),
	}
	req.LoadEntries(newEntries)

	require.Len(t, req.Entries, 2)
	require.Same(t, newEntries[0], req.Entries[0])
	require.Equal(t, newEntries[0].Key, req.Entries[0].Key)

	req.Reset()
	require.Empty(t, req.Entries)
	require.Empty(t, req.Ptrs)
	require.Empty(t, req.PtrIdxs)
	require.Empty(t, req.PtrBuckets)
	require.NoError(t, req.Err)
	require.True(t, req.EnqueueAt.IsZero())
	require.EqualValues(t, 0, req.RefCount.Load())

	for _, entry := range append([]*kv.Entry{oldEntry}, newEntries...) {
		entry.DecrRef()
	}
}

func TestRequestWaitReleasesEntries(t *testing.T) {
	entry := kv.NewEntry([]byte("k"), []byte("v"))
	req := &Request{
		Entries: []*kv.Entry{entry},
		Err:     errors.New("commit failed"),
	}
	req.IncrRef()
	req.WG.Add(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(10 * time.Millisecond)
		req.WG.Done()
	}()

	err := req.Wait()
	<-done

	require.EqualError(t, err, "commit failed")
	require.Nil(t, req.Entries)
	require.Nil(t, req.Ptrs)
	require.Nil(t, req.PtrIdxs)
	require.Nil(t, req.PtrBuckets)
	require.EqualValues(t, 0, entry.Load())
}

func TestCommitQueueLifecycleAndAccounting(t *testing.T) {
	var queue CommitQueue
	queue.Init(2)

	require.False(t, queue.Closed())
	require.Equal(t, 0, queue.Len())

	consumer := queue.Consumer()
	require.NotNil(t, consumer)
	defer consumer.Close()

	req1 := &CommitRequest{EntryCount: 1, Size: 10}
	req2 := &CommitRequest{EntryCount: 2, Size: 20}
	require.True(t, queue.Push(req1))
	require.True(t, queue.Push(req2))
	require.GreaterOrEqual(t, queue.Len(), 2)

	queue.AddPending(3, 30)
	require.EqualValues(t, 3, queue.PendingEntries())
	require.EqualValues(t, 30, queue.PendingBytes())

	first := queue.Pop(consumer)
	require.Same(t, req1, first)

	var drained []*CommitRequest
	n := queue.DrainReady(consumer, 4, func(cr *CommitRequest) bool {
		drained = append(drained, cr)
		return true
	})
	require.Equal(t, 1, n)
	require.Equal(t, []*CommitRequest{req2}, drained)

	require.True(t, queue.Close())
	require.True(t, queue.Closed())
	require.False(t, queue.Close())
	select {
	case <-queue.CloseCh():
	default:
		t.Fatal("expected close channel to be closed")
	}
	require.False(t, queue.Push(&CommitRequest{}))
	require.Nil(t, queue.Pop(nil))
}
