package storage

import (
	"context"
	"errors"
	"math"
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestResolveAllocatorStartsBasic(t *testing.T) {
	id, ts := ResolveAllocatorStarts(1, 100, AllocatorState{
		IDCurrent: 50,
		TSCurrent: 20,
	})
	require.Equal(t, uint64(51), id)
	require.Equal(t, uint64(100), ts)

	id, ts = ResolveAllocatorStarts(80, 30, AllocatorState{
		IDCurrent: 50,
		TSCurrent: 20,
	})
	require.Equal(t, uint64(80), id)
	require.Equal(t, uint64(30), ts)
}

func TestResolveAllocatorStartsHandlesUint64Overflow(t *testing.T) {
	id, ts := ResolveAllocatorStarts(1, 1, AllocatorState{
		IDCurrent: math.MaxUint64,
		TSCurrent: math.MaxUint64,
	})
	require.Equal(t, uint64(math.MaxUint64), id)
	require.Equal(t, uint64(math.MaxUint64), ts)
}

func TestRestoreDescriptorsOrdersAndSkipsInvalidEntries(t *testing.T) {
	var applied []uint64
	loaded, err := RestoreDescriptors(func(desc descriptor.Descriptor) error {
		applied = append(applied, desc.RegionID)
		return nil
	}, map[uint64]descriptor.Descriptor{
		0: {RegionID: 0},
		3: {RegionID: 3},
		1: {RegionID: 1},
		2: {RegionID: 0},
	})
	require.NoError(t, err)
	require.Equal(t, 2, loaded)
	require.Equal(t, []uint64{1, 3}, applied)
}

func TestRestoreDescriptorsReturnsLoadedCountOnApplyError(t *testing.T) {
	boom := errors.New("boom")
	applied := 0
	loaded, err := RestoreDescriptors(func(desc descriptor.Descriptor) error {
		applied++
		if desc.RegionID == 2 {
			return boom
		}
		return nil
	}, map[uint64]descriptor.Descriptor{
		1: {RegionID: 1},
		2: {RegionID: 2},
		3: {RegionID: 3},
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, loaded)
	require.Equal(t, 2, applied)
}

func TestBootstrapRestoresSnapshotAndAllocatorStarts(t *testing.T) {
	store := bootstrapTestStore{snapshot: Snapshot{
		ClusterEpoch: 7,
		Descriptors: map[uint64]descriptor.Descriptor{
			9: {RegionID: 9},
			3: {RegionID: 3},
		},
		Allocator: AllocatorState{
			IDCurrent: 40,
			TSCurrent: 80,
		},
	}}

	var applied []uint64
	info, err := Bootstrap(store, func(desc descriptor.Descriptor) error {
		applied = append(applied, desc.RegionID)
		return nil
	}, 10, 20)
	require.NoError(t, err)
	require.Equal(t, 2, info.LoadedRegions)
	require.Equal(t, uint64(41), info.IDStart)
	require.Equal(t, uint64(81), info.TSStart)
	require.Equal(t, []uint64{3, 9}, applied)
	require.Equal(t, uint64(7), info.Snapshot.ClusterEpoch)
}

func TestBootstrapPropagatesLoadAndRestoreErrors(t *testing.T) {
	loadErr := errors.New("load failed")
	_, err := Bootstrap(bootstrapTestStore{loadErr: loadErr}, nil, 1, 2)
	require.ErrorIs(t, err, loadErr)

	restoreErr := errors.New("restore failed")
	_, err = Bootstrap(bootstrapTestStore{snapshot: Snapshot{
		Descriptors: map[uint64]descriptor.Descriptor{1: {RegionID: 1}},
	}}, func(descriptor.Descriptor) error {
		return restoreErr
	}, 1, 2)
	require.ErrorIs(t, err, restoreErr)
}

type bootstrapTestStore struct {
	snapshot Snapshot
	loadErr  error
}

func (s bootstrapTestStore) Load() (Snapshot, error) {
	if s.loadErr != nil {
		return Snapshot{}, s.loadErr
	}
	return CloneSnapshot(s.snapshot), nil
}

func (bootstrapTestStore) AppendRootEvent(context.Context, rootevent.Event) error { return nil }
func (bootstrapTestStore) SaveAllocatorState(context.Context, uint64, uint64) error {
	return nil
}
func (bootstrapTestStore) ApplyCoordinatorLease(context.Context, rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	return rootstate.CoordinatorProtocolState{}, nil
}
func (bootstrapTestStore) ApplyCoordinatorClosure(context.Context, rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	return rootstate.CoordinatorProtocolState{}, nil
}
func (bootstrapTestStore) Refresh() error   { return nil }
func (bootstrapTestStore) IsLeader() bool   { return true }
func (bootstrapTestStore) LeaderID() uint64 { return 0 }
func (bootstrapTestStore) Close() error     { return nil }
