package storage

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

type fakeSubstrate struct {
	checkpoint Checkpoint
	tail       CommittedTail
}

func (f fakeSubstrate) LoadCheckpoint() (Checkpoint, error) {
	return CloneCheckpoint(f.checkpoint), nil
}

func (f fakeSubstrate) SaveCheckpoint(Checkpoint) error { return nil }

func (f fakeSubstrate) ReadCommitted(int64) (CommittedTail, error) {
	return CloneCommittedTail(f.tail), nil
}

func (f fakeSubstrate) AppendCommitted(...CommittedEvent) (int64, error) { return 0, nil }

func (f fakeSubstrate) CompactCommitted(CommittedTail) error { return nil }

func (f fakeSubstrate) InstallBootstrap(ObservedCommitted) error { return nil }

func (f fakeSubstrate) Size() (int64, error) { return 0, nil }

func TestObserveCommittedDerivesLastCursorAndRetainFrom(t *testing.T) {
	storage := fakeSubstrate{
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

	observed, err := ObserveCommitted(storage, 0)
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
	require.True(t, advance.ShouldRefreshState())
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
	require.False(t, advance.ShouldRefreshState())
	require.True(t, advance.FellBehind())
	require.Equal(t, TailWindow{RequestedOffset: 10, StartOffset: 12, EndOffset: 16}, advance.Window())
	require.False(t, advance.Window().Empty())
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
