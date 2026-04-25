package watch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
)

func TestRouterFiltersByPrefixAndAcksBudget(t *testing.T) {
	router := NewRouter()
	prefix := []byte("fsm\x00prefix/")
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: 1,
	})
	require.NoError(t, err)
	defer sub.Close()

	router.Publish(fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		CommitVersion: 10,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("other/key"),
	})
	select {
	case <-sub.Events():
		t.Fatal("unexpected event for non-matching prefix")
	default:
	}

	want := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 4},
		CommitVersion: 11,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("fsm\x00prefix/a"),
	}
	router.Publish(want)
	require.Equal(t, want, <-sub.Events())
	sub.Ack(want.Cursor)

	next := want
	next.Cursor.Index = 5
	next.CommitVersion = 12
	next.Key = []byte("fsm\x00prefix/b")
	router.Publish(next)
	require.Equal(t, next, <-sub.Events())
	require.NoError(t, sub.Err())
}

func TestRouterClosesSlowSubscriberOnOverflow(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          []byte("k/"),
		BackPressureWindow: 1,
	})
	require.NoError(t, err)

	router.Publish(fsmeta.WatchEvent{Cursor: fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 1}, Key: []byte("k/a")})
	router.Publish(fsmeta.WatchEvent{Cursor: fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 2}, Key: []byte("k/b")})

	require.Eventually(t, func() bool {
		return errors.Is(sub.Err(), fsmeta.ErrWatchOverflow)
	}, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, router.Dropped(), uint64(1))
}

func TestRouterConsumesRaftstoreApplyEvents(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix: []byte("dentry/"),
	})
	require.NoError(t, err)
	defer sub.Close()

	router.OnApply(storepkg.ApplyEvent{
		RegionID:      7,
		Term:          3,
		Index:         9,
		Source:        storepkg.ApplyEventSourceResolveLock,
		CommitVersion: 44,
		Keys:          [][]byte{[]byte("dentry/a"), []byte("inode/1")},
	})

	got := <-sub.Events()
	require.Equal(t, fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 7, Term: 3, Index: 9},
		CommitVersion: 44,
		Source:        fsmeta.WatchEventSourceResolveLock,
		Key:           []byte("dentry/a"),
	}, got)
}

func TestRouterDeduplicatesReplicatedApplyEvents(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix: []byte("k/"),
	})
	require.NoError(t, err)
	defer sub.Close()

	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		CommitVersion: 4,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/a"),
	}
	router.Publish(evt)
	router.Publish(evt)

	require.Equal(t, evt, <-sub.Events())
	select {
	case got := <-sub.Events():
		t.Fatalf("unexpected duplicate event: %+v", got)
	default:
	}
}

func TestRouterReplaysEventsAfterResumeCursor(t *testing.T) {
	router := NewRouter()
	prefix := []byte("k/")
	first := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 1},
		CommitVersion: 10,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/a"),
	}
	second := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 2},
		CommitVersion: 11,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/b"),
	}
	thirdOtherPrefix := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 3},
		CommitVersion: 12,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("other/c"),
	}
	router.Publish(first)
	router.Publish(second)
	router.Publish(thirdOtherPrefix)

	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:    prefix,
		ResumeCursor: first.Cursor,
	})
	require.NoError(t, err)
	defer sub.Close()
	require.Equal(t, thirdOtherPrefix.Cursor, sub.ReadyCursor())
	require.Equal(t, second, <-sub.Events())
	select {
	case got := <-sub.Events():
		t.Fatalf("unexpected extra replay event: %+v", got)
	default:
	}

	live := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 4},
		CommitVersion: 13,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/d"),
	}
	router.Publish(live)
	require.Equal(t, live, <-sub.Events())
}

func TestRouterRejectsExpiredResumeCursor(t *testing.T) {
	router := NewRouter()
	router.Publish(fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 10},
		CommitVersion: 20,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/current"),
	})

	_, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:    []byte("k/"),
		ResumeCursor: fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 9},
	})
	require.ErrorIs(t, err, fsmeta.ErrWatchCursorExpired)
}

func TestWatchPrefixRejectsRecursiveInodeSubtree(t *testing.T) {
	_, err := fsmeta.WatchPrefix(fsmeta.WatchRequest{
		Mount:              "vol",
		RootInode:          fsmeta.RootInode,
		DescendRecursively: true,
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}
