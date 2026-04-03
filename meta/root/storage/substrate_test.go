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

func (f fakeSubstrate) InstallBootstrap(Checkpoint, CommittedTail) error { return nil }

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

	advance := TailAdvance{
		Token:    TailToken{Cursor: observed.LastCursor(), Revision: 2},
		Observed: observed,
	}
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 6}, advance.LastCursor())
	require.True(t, advance.FellBehind())
}
