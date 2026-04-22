package view

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegionDirectoryViewLookupAndValidation(t *testing.T) {
	v := NewRegionDirectoryView()
	now := time.Unix(200, 0)
	require.NoError(t, v.UpsertAt(testViewDescriptor(1, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}), now))
	require.NoError(t, v.UpsertAt(testViewDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 1, ConfVersion: 1}), now))

	got, ok := v.LookupDescriptor([]byte("a"))
	require.True(t, ok)
	require.Equal(t, uint64(1), got.RegionID)

	got, ok = v.LookupDescriptor([]byte("m"))
	require.True(t, ok)
	require.Equal(t, uint64(2), got.RegionID)

	ts, ok := v.LastHeartbeat(2)
	require.True(t, ok)
	require.Equal(t, now, ts)

	err := v.UpsertAt(testViewDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 0, ConfVersion: 1}), now)
	require.ErrorIs(t, err, ErrRegionHeartbeatStale)

	err = v.UpsertAt(testViewDescriptor(3, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}), now)
	require.ErrorIs(t, err, ErrRegionRangeOverlap)
}

func TestRegionDirectoryViewReplaceTouchAndRemove(t *testing.T) {
	v := NewRegionDirectoryView()
	first := time.Unix(300, 0)
	second := first.Add(time.Minute)

	require.NoError(t, v.UpsertAt(testViewDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}), first))
	v.RecordLeader(1, 2, first)
	v.Replace(map[uint64]descriptor.Descriptor{
		2: testViewDescriptor(2, []byte("m"), []byte("z"), metaregion.Epoch{Version: 2, ConfVersion: 1}),
	})

	_, ok := v.LookupDescriptor([]byte("b"))
	require.False(t, ok)
	got, ok := v.LookupDescriptor([]byte("x"))
	require.True(t, ok)
	require.Equal(t, uint64(2), got.RegionID)

	require.True(t, v.Touch(2, second))
	ts, ok := v.LastHeartbeat(2)
	require.True(t, ok)
	require.Equal(t, second, ts)
	require.False(t, v.Touch(9, second))

	desc, ok := v.Descriptor(2)
	require.True(t, ok)
	desc.StartKey = []byte("changed")
	fresh, ok := v.Descriptor(2)
	require.True(t, ok)
	require.Equal(t, []byte("m"), fresh.StartKey)

	snap := v.Snapshot()
	require.Len(t, snap, 1)
	require.Zero(t, snap[0].LeaderStoreID)

	require.True(t, v.Remove(2))
	require.False(t, v.Remove(2))
}

func TestRegionDirectoryViewLeaderClaimsTrackRemovalAndReplace(t *testing.T) {
	v := NewRegionDirectoryView()
	now := time.Unix(500, 0)
	require.NoError(t, v.UpsertAt(testViewDescriptor(1, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}), now))
	require.NoError(t, v.UpsertAt(testViewDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 1, ConfVersion: 1}), now))

	v.RecordLeader(1, 3, now)
	v.RecordLeader(2, 4, now)
	v.Remove(1)

	snap := v.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(2), snap[0].Descriptor.RegionID)
	require.Equal(t, uint64(4), snap[0].LeaderStoreID)

	v.Replace(map[uint64]descriptor.Descriptor{
		3: testViewDescriptor(3, []byte(""), []byte(""), metaregion.Epoch{Version: 2, ConfVersion: 1}),
	})
	snap = v.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(3), snap[0].Descriptor.RegionID)
	require.Zero(t, snap[0].LeaderStoreID)
}

func TestRegionDirectoryViewRejectsInvalidRegionID(t *testing.T) {
	v := NewRegionDirectoryView()
	err := v.UpsertAt(descriptor.Descriptor{}, time.Unix(400, 0))
	require.ErrorIs(t, err, ErrInvalidRegionID)
}

func TestRegionDirectoryViewValidateDescriptorsSnapshotAndLeaderCleanup(t *testing.T) {
	v := NewRegionDirectoryView()
	now := time.Unix(700, 0)
	left := testViewDescriptor(1, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1})
	right := testViewDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 1, ConfVersion: 1})

	require.NoError(t, v.Upsert(left))
	require.NoError(t, v.UpsertAt(right, now))

	require.NoError(t, v.Validate(testViewDescriptor(2, []byte("m"), []byte(""), metaregion.Epoch{Version: 2, ConfVersion: 1})))
	require.ErrorIs(t, v.Validate(testViewDescriptor(3, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1})), ErrRegionRangeOverlap)

	snapshot := v.DescriptorsSnapshot()
	require.Len(t, snapshot, 2)
	snapshot[1] = testViewDescriptor(9, []byte("x"), []byte("y"), metaregion.Epoch{Version: 1, ConfVersion: 1})

	desc, ok := v.Descriptor(1)
	require.True(t, ok)
	require.Equal(t, uint64(1), desc.RegionID)

	v.RecordLeader(1, 3, now)
	v.RecordLeader(2, 3, now)
	v.ClearLeadersFromStore(3, map[uint64]struct{}{2: {}})

	snap := v.Snapshot()
	require.Len(t, snap, 2)
	require.Zero(t, snap[0].LeaderStoreID)
	require.Equal(t, uint64(3), snap[1].LeaderStoreID)
}

func testViewDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    epoch,
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
