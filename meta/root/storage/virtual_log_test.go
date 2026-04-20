package storage

import (
	"context"
	"testing"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

type fakeVirtualLog struct {
	checkpoint Checkpoint
	tail       CommittedTail
}

func (f fakeVirtualLog) LoadCheckpoint() (Checkpoint, error) {
	return CloneCheckpoint(f.checkpoint), nil
}

func (f fakeVirtualLog) SaveCheckpoint(Checkpoint) error { return nil }

func (f fakeVirtualLog) ReadCommitted(int64) (CommittedTail, error) {
	return CloneCommittedTail(f.tail), nil
}

func (f fakeVirtualLog) AppendCommitted(context.Context, ...CommittedEvent) (int64, error) {
	return 0, nil
}

func (f fakeVirtualLog) CompactCommitted(CommittedTail) error { return nil }

func (f fakeVirtualLog) InstallBootstrap(ObservedCommitted) error { return nil }

func (f fakeVirtualLog) Size() (int64, error) { return 0, nil }

func TestObserveCommittedDerivesLastCursorAndRetainFrom(t *testing.T) {
	log := fakeVirtualLog{
		checkpoint: Checkpoint{
			Snapshot: rootstate.Snapshot{
				State: rootstate.State{
					LastCommitted: rootstate.Cursor{Term: 2, Index: 4},
				},
			},
			TailOffset: 32,
		},
		tail: CommittedTail{
			RequestedOffset: 0,
			StartOffset:     32,
			EndOffset:       64,
			Records: []CommittedEvent{
				{Cursor: rootstate.Cursor{Term: 2, Index: 5}, Event: rootevent.StoreJoined(1, "s1")},
				{Cursor: rootstate.Cursor{Term: 2, Index: 6}, Event: rootevent.StoreJoined(2, "s2")},
			},
		},
	}

	observed, err := ObserveCommitted(log, 0)
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 6}, observed.LastCursor())
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 4}, observed.RetainFrom())
	require.Equal(t, TailWindow{RequestedOffset: 0, StartOffset: 32, EndOffset: 64}, observed.Window())

	advance := TailAdvance{
		After:    TailToken{Cursor: rootstate.Cursor{Term: 2, Index: 5}, Revision: 1},
		Token:    TailToken{Cursor: observed.LastCursor(), Revision: 2},
		Observed: observed,
	}
	require.True(t, advance.Advanced())
	require.True(t, advance.CursorAdvanced())
	require.False(t, advance.WindowShifted())
	require.Equal(t, TailAdvanceCursorAdvanced, advance.Kind())
	require.Equal(t, TailCatchUpRefreshState, advance.CatchUpAction())
	require.False(t, advance.NeedsBootstrapInstall())
	require.True(t, advance.ShouldReloadState())
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 6}, advance.LastCursor())
	require.True(t, advance.FellBehind())
}

func TestTailAdvanceClassifiesWindowShiftWithoutCursorAdvance(t *testing.T) {
	advance := TailAdvance{
		After: TailToken{
			Cursor:   rootstate.Cursor{Term: 3, Index: 9},
			Revision: 4,
		},
		Token: TailToken{
			Cursor:   rootstate.Cursor{Term: 3, Index: 9},
			Revision: 5,
		},
		Observed: ObservedCommitted{
			Checkpoint: Checkpoint{
				Snapshot: rootstate.Snapshot{
					State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 3, Index: 9}},
				},
			},
			Tail: CommittedTail{RequestedOffset: 10, StartOffset: 12, EndOffset: 16},
		},
	}
	require.True(t, advance.Advanced())
	require.False(t, advance.CursorAdvanced())
	require.True(t, advance.WindowShifted())
	require.Equal(t, TailAdvanceWindowShifted, advance.Kind())
	require.Equal(t, TailCatchUpAcknowledgeWindow, advance.CatchUpAction())
	require.False(t, advance.NeedsBootstrapInstall())
	require.False(t, advance.ShouldReloadState())
	require.True(t, advance.FellBehind())
	require.Equal(t, TailWindow{RequestedOffset: 10, StartOffset: 12, EndOffset: 16}, advance.Window())
	require.False(t, advance.Window().Empty())
}

func TestTailAdvanceDetectsBootstrapInstall(t *testing.T) {
	advance := TailAdvance{
		After: TailToken{
			Cursor:   rootstate.Cursor{Term: 3, Index: 7},
			Revision: 4,
		},
		Token: TailToken{
			Cursor:   rootstate.Cursor{Term: 3, Index: 9},
			Revision: 5,
		},
		Observed: ObservedCommitted{
			Checkpoint: Checkpoint{
				Snapshot: rootstate.Snapshot{
					State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 3, Index: 9}},
				},
			},
			Tail: CommittedTail{
				RequestedOffset: 0,
				StartOffset:     12,
				EndOffset:       16,
				Records: []CommittedEvent{
					{Cursor: rootstate.Cursor{Term: 3, Index: 9}, Event: rootevent.StoreJoined(1, "a")},
				},
			},
		},
	}
	require.True(t, advance.NeedsBootstrapInstall())
	require.Equal(t, TailCatchUpInstallBootstrap, advance.CatchUpAction())
	require.True(t, advance.ShouldReloadState())
}

func TestObservedCommittedInstallableResetsTailOrigin(t *testing.T) {
	observed := ObservedCommitted{
		Checkpoint: Checkpoint{
			Snapshot:   rootstate.Snapshot{State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 1, Index: 3}}},
			TailOffset: 48,
		},
		Tail: CommittedTail{
			RequestedOffset: 16,
			StartOffset:     48,
			EndOffset:       96,
			Records: []CommittedEvent{
				{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Event: rootevent.StoreJoined(1, "a")},
			},
		},
	}
	installable := observed.Installable()
	require.Equal(t, int64(0), installable.Checkpoint.TailOffset)
	require.Equal(t, int64(0), installable.Tail.RequestedOffset)
	require.Equal(t, int64(0), installable.Tail.StartOffset)
	require.Equal(t, int64(0), installable.Tail.EndOffset)
	require.Len(t, installable.Tail.Records, 1)
}

func TestPlanTailCompaction(t *testing.T) {
	records := []CommittedEvent{
		{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Event: rootevent.StoreJoined(1, "a")},
		{Cursor: rootstate.Cursor{Term: 1, Index: 4}, Event: rootevent.StoreJoined(2, "b")},
		{Cursor: rootstate.Cursor{Term: 1, Index: 5}, Event: rootevent.StoreJoined(3, "c")},
	}
	plan := PlanTailCompaction(records, rootstate.Cursor{Term: 1, Index: 5}, 2)
	require.True(t, plan.Compacted)
	require.Len(t, plan.Tail.Records, 2)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, plan.RetainFrom)

	plan = PlanTailCompaction(records, rootstate.Cursor{Term: 1, Index: 5}, 4)
	require.False(t, plan.Compacted)
	require.Len(t, plan.Tail.Records, 3)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, plan.RetainFrom)
}

func TestTailSubscriptionTracksAcknowledgedToken(t *testing.T) {
	initial := TailToken{Cursor: rootstate.Cursor{Term: 2, Index: 4}, Revision: 3}
	expected := TailAdvance{
		After: initial,
		Token: TailToken{Cursor: rootstate.Cursor{Term: 2, Index: 5}, Revision: 4},
		Observed: ObservedCommitted{
			Checkpoint: Checkpoint{
				Snapshot: rootstate.Snapshot{
					State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 2, Index: 5}},
				},
			},
		},
	}

	var seen TailToken
	sub := NewTailSubscription(initial, func(after TailToken, timeout time.Duration) (TailAdvance, error) {
		seen = after
		return expected, nil
	})
	require.NotNil(t, sub)

	advance, err := sub.Wait(50 * time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, initial, seen)
	require.Equal(t, expected.Token, advance.Token)

	sub.Acknowledge(advance)
	require.Equal(t, expected.Token, sub.Token())
}

func TestWatchedTailSubscriptionUsesWatchChannel(t *testing.T) {
	initial := TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 4}, Revision: 3}
	watch := make(chan struct{}, 1)
	next := TailAdvance{
		After: initial,
		Token: TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 5}, Revision: 4},
		Observed: ObservedCommitted{
			Checkpoint: Checkpoint{
				Snapshot: rootstate.Snapshot{
					State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 1, Index: 5}},
				},
			},
		},
	}
	calls := 0
	sub := NewWatchedTailSubscription(
		initial,
		func(after TailToken) (TailAdvance, error) {
			calls++
			if calls == 1 {
				return TailAdvance{After: after, Token: after}, nil
			}
			return next, nil
		},
		watch,
		nil,
	)
	require.NotNil(t, sub)
	done := make(chan TailAdvance, 1)
	go func() {
		advance, err := sub.Next(context.Background(), time.Second)
		require.NoError(t, err)
		done <- advance
	}()
	time.Sleep(20 * time.Millisecond)
	watch <- struct{}{}
	advance := <-done
	require.Equal(t, next.Token, advance.Token)
}
