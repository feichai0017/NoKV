package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
