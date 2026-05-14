// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import (
	"context"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNetworkDriverCompactCommittedShiftsTailWindow(t *testing.T) {
	_, drivers, leaderID := openNetworkTestCluster(t, 8)
	driver := drivers[leaderID]
	records := make([]rootstorage.CommittedEvent, 0, 3)
	for i := range 3 {
		records = append(records, rootstorage.CommittedEvent{
			Cursor: rootstate.Cursor{Term: 1, Index: uint64(i + 1)},
			Event: rootevent.RegionDescriptorPublished(testDescriptor(
				uint64(200+i),
				[]byte{byte('a' + i)},
				[]byte{byte('b' + i)},
			)),
		})
	}
	_, err := driver.AppendCommitted(context.Background(), records...)
	require.NoError(t, err)

	before, err := driver.ObserveTail(rootstorage.TailToken{})
	require.NoError(t, err)
	require.True(t, before.Advanced())

	sizeBefore, err := driver.Size()
	require.NoError(t, err)
	require.Greater(t, sizeBefore, int64(0))

	stream := before.Observed.Tail
	require.Len(t, stream.Records, 3)

	compacted := rootstorage.CommittedTail{Records: rootstorage.CloneCommittedEvents(stream.Records[2:])}
	require.NoError(t, driver.CompactCommitted(compacted))

	sizeAfter, err := driver.Size()
	require.NoError(t, err)
	require.Greater(t, sizeAfter, int64(0))

	after, err := driver.ObserveTail(before.Token)
	require.NoError(t, err)
	require.True(t, after.Advanced())
	require.True(t, after.WindowShifted())
	require.Equal(t, rootstorage.TailAdvanceWindowShifted, after.Kind())
	require.Equal(t, before.LastCursor(), after.LastCursor())

	require.Len(t, after.Observed.Tail.Records, 1)
	require.Equal(t, stream.Records[2].Cursor, after.Observed.Tail.Records[0].Cursor)
}

func TestNetworkDriverAppendCommittedWaitsForCommittedTail(t *testing.T) {
	_, drivers, leaderID := openNetworkTestCluster(t, 8)
	driver := drivers[leaderID]

	before, err := driver.ObserveTail(rootstorage.TailToken{})
	require.NoError(t, err)

	records := []rootstorage.CommittedEvent{
		{
			Cursor: rootstate.Cursor{Term: 1, Index: 1},
			Event:  rootevent.RegionDescriptorPublished(testDescriptor(301, []byte("a"), []byte("b"))),
		},
		{
			Cursor: rootstate.Cursor{Term: 1, Index: 2},
			Event:  rootevent.RegionDescriptorPublished(testDescriptor(302, []byte("b"), []byte("c"))),
		},
	}
	logEnd, err := driver.AppendCommitted(context.Background(), records...)
	require.NoError(t, err)
	require.Greater(t, logEnd, int64(0))

	after, err := driver.ObserveTail(before.Token)
	require.NoError(t, err)
	require.True(t, after.Advanced())
	require.Equal(t, records[len(records)-1].Cursor, after.LastCursor())
	require.Len(t, after.Observed.Tail.Records, 2)
	require.Equal(t, records[len(records)-1].Cursor, after.Observed.Tail.Records[len(after.Observed.Tail.Records)-1].Cursor)
}

func TestNetworkDriverAppendCommittedRejectsStaleCursor(t *testing.T) {
	_, drivers, leaderID := openNetworkTestCluster(t, 8)
	driver := drivers[leaderID]

	first := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 1},
		Event:  rootevent.StoreJoined(1),
	}
	_, err := driver.AppendCommitted(context.Background(), first)
	require.NoError(t, err)

	_, err = driver.AppendCommitted(context.Background(), first)
	require.ErrorContains(t, err, "committed cursor discontinuity")

	after, err := driver.ObserveTail(rootstorage.TailToken{})
	require.NoError(t, err)
	require.Equal(t, first.Cursor, after.LastCursor())
	require.Len(t, after.Observed.Tail.Records, 1)
}
