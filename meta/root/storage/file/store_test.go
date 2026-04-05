package file

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/vfs"
	"github.com/stretchr/testify/require"
)

func TestStoreReadCommittedReportsFellBehindCompaction(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(vfs.Ensure(nil), dir)

	rec1 := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 1},
		Event:  rootevent.StoreJoined(1, "s1"),
	}
	rec2 := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 2},
		Event:  rootevent.StoreJoined(2, "s2"),
	}
	rec3 := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 3},
		Event:  rootevent.StoreJoined(3, "s3"),
	}

	offsetAfterFirst, err := store.AppendCommitted(rec1)
	require.NoError(t, err)
	_, err = store.AppendCommitted(rec2, rec3)
	require.NoError(t, err)

	require.NoError(t, store.SaveCheckpoint(rootstorage.Checkpoint{
		Snapshot:   rootstate.Snapshot{},
		TailOffset: offsetAfterFirst,
	}))

	tail, err := store.ReadCommitted(0)
	require.NoError(t, err)
	require.True(t, tail.FellBehind())
	require.Equal(t, int64(0), tail.RequestedOffset)
	require.Equal(t, offsetAfterFirst, tail.StartOffset)
	require.GreaterOrEqual(t, tail.EndOffset, tail.StartOffset)
	require.Len(t, tail.Records, 2)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, tail.Records[0].Cursor)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, tail.TailCursor(rootstate.Cursor{}))
}

func TestStoreInstallBootstrapNormalizesTailOrigin(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(vfs.Ensure(nil), dir)

	observed := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{
			Snapshot:   rootstate.Snapshot{State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 1, Index: 2}}},
			TailOffset: 64,
		},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 32,
			StartOffset:     64,
			EndOffset:       96,
			Records: []rootstorage.CommittedEvent{
				{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Event: rootevent.StoreJoined(2, "s2")},
			},
		},
	}
	require.NoError(t, store.InstallBootstrap(observed))

	checkpoint, err := store.LoadCheckpoint()
	require.NoError(t, err)
	require.Equal(t, int64(0), checkpoint.TailOffset)

	tail, err := store.ReadCommitted(0)
	require.NoError(t, err)
	require.Len(t, tail.Records, 1)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, tail.Records[0].Cursor)
}
