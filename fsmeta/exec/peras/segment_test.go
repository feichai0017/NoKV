package peras

import (
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestBuildPerasSegmentBuildsSortedQueryableRuns(t *testing.T) {
	plan := workspaceCreateReplayPlan(t, 3)

	segment, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)

	require.Equal(t, uint64(11), segment.EpochID)
	require.Equal(t, ReplayVersionRange{First: 1000, Count: 3}, segment.Versions)
	require.NotZero(t, segment.Root)
	require.Len(t, segment.Dentries, 3)
	require.Len(t, segment.Inodes, 3)
	require.Empty(t, segment.Tombstones)

	firstDentry := segment.Dentries[0]
	value, deleted, ok := segment.Get(firstDentry.Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, firstDentry.Value, value)

	entries := segment.Entries()
	require.Len(t, entries, 6)
	for i := 1; i < len(entries); i++ {
		require.LessOrEqual(t, string(entries[i-1].Key), string(entries[i].Key))
	}
}

func TestBuildPerasSegmentCoalescesLatestValueAndDelete(t *testing.T) {
	key := []byte("workspace/run/checkpoint/file")
	plan := ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{
			{
				OpID: OperationID{ClientID: "client-a", Seq: 1},
				Kind: fsmeta.OperationCreate,
				Mutations: []ReplayMutation{
					{Key: key, Value: []byte("v1")},
				},
			},
			{
				OpID: OperationID{ClientID: "client-a", Seq: 2},
				Kind: fsmeta.OperationUpdateInode,
				Mutations: []ReplayMutation{
					{Key: key, Value: []byte("v2")},
				},
			},
			{
				OpID: OperationID{ClientID: "client-a", Seq: 3},
				Kind: fsmeta.OperationUnlink,
				Mutations: []ReplayMutation{
					{Key: key, Delete: true},
				},
			},
		},
	}

	segment, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)

	_, deleted, ok := segment.Get(key)
	require.True(t, ok)
	require.True(t, deleted)
	require.Len(t, segment.Tombstones, 1)
	require.Empty(t, segment.Other)
	stats := segment.Stats()
	require.Equal(t, uint64(3), stats.InputMutationCount)
	require.Equal(t, uint64(1), stats.EntryCount)
	require.Equal(t, uint64(2), stats.CoalescedMutations)
	require.Equal(t, 3.0, stats.CompressionRatio)
}

func TestBuildPerasSegmentRecordsCompletionAndVersions(t *testing.T) {
	plan := workspaceCreateReplayPlan(t, 2)

	segment, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)

	first, ok := segment.Completion(OperationID{ClientID: "workspace-writer", Seq: 1})
	require.True(t, ok)
	require.Equal(t, fsmeta.OperationCreate, first.Kind)
	require.Equal(t, uint64(1000), first.Version)
	require.Equal(t, uint32(2), first.MutationCount)

	second, ok := segment.Completion(OperationID{ClientID: "workspace-writer", Seq: 2})
	require.True(t, ok)
	require.Equal(t, uint64(1001), second.Version)
}

func TestBuildPerasSegmentRootIsStableAndSensitive(t *testing.T) {
	plan := workspaceCreateReplayPlan(t, 4)
	left, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	right, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	require.Equal(t, left.Root, right.Root)

	plan.Operations[0].Mutations[0].Value = []byte("changed")
	changed, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	require.NotEqual(t, left.Root, changed.Root)
}

func TestBuildPerasSegmentRejectsInvalidPlans(t *testing.T) {
	_, err := BuildPerasSegmentFromReplayPlan(ReplayPlan{})
	require.ErrorIs(t, err, ErrInvalidPerasSeal)

	_, err = BuildPerasSegmentFromReplayPlan(ReplayPlan{
		EpochID:  1,
		Versions: ReplayVersionRange{First: 1, Count: 2},
		Operations: []ReplayOperation{
			{OpID: OperationID{ClientID: "c", Seq: 1}, Kind: fsmeta.OperationCreate, Mutations: []ReplayMutation{{Key: []byte("k"), Value: []byte("v")}}},
		},
	})
	require.ErrorIs(t, err, ErrReplayVersionRequired)

	_, err = BuildPerasSegmentFromReplayPlan(ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{
			{OpID: OperationID{ClientID: "c", Seq: 1}, Kind: fsmeta.OperationCreate, Mutations: []ReplayMutation{{Key: []byte("k")}}},
		},
	})
	require.ErrorIs(t, err, ErrInvalidPerasSeal)
}

func BenchmarkBuildWorkspaceSegment1000(b *testing.B) {
	plan := workspaceCreateReplayPlan(b, 1000)

	b.ReportAllocs()
	for b.Loop() {
		segment, err := BuildPerasSegmentFromReplayPlan(plan)
		if err != nil {
			b.Fatal(err)
		}
		if len(segment.Dentries) == 0 {
			b.Fatal("empty segment")
		}
	}
}

func BenchmarkSegmentLookup(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	key := segment.Dentries[len(segment.Dentries)/2].Key

	b.ReportAllocs()
	for b.Loop() {
		_, _, ok := segment.Get(key)
		if !ok {
			b.Fatal("missing key")
		}
	}
}

func BenchmarkSegmentLookupView(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	key := segment.Dentries[len(segment.Dentries)/2].Key

	b.ReportAllocs()
	for b.Loop() {
		_, _, ok := segment.GetView(key)
		if !ok {
			b.Fatal("missing key")
		}
	}
}

func BenchmarkSegmentScan(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	start := segment.Dentries[0].Key

	b.ReportAllocs()
	for b.Loop() {
		out := segment.Scan(start, 128)
		if len(out) == 0 {
			b.Fatal("empty scan")
		}
	}
}

func BenchmarkSegmentScanView(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	start := segment.Dentries[0].Key

	b.ReportAllocs()
	for b.Loop() {
		out := segment.ScanView(start, 128)
		if len(out) == 0 {
			b.Fatal("empty scan")
		}
	}
}

func workspaceCreateReplayPlan(tb testing.TB, count int) ReplayPlan {
	tb.Helper()
	mount := fsmeta.MountIdentity{MountID: "workspace", MountKeyID: 42}
	ops := make([]ReplayOperation, 0, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("checkpoint-%06d", i)
		delta, err := compile.Create(fsmeta.CreateRequest{
			Mount:  mount.MountID,
			Parent: fsmeta.RootInode,
			Name:   name,
			Attrs: fsmeta.CreateAttrs{
				Type:          fsmeta.InodeTypeFile,
				Size:          uint64(i),
				Mode:          0o644,
				CreatedUnixNs: int64(i + 1),
				UpdatedUnixNs: int64(i + 1),
			},
		}, mount, fsmeta.InodeID(1000+i))
		require.NoError(tb, err)
		op, err := replayOperationFromDelta(OperationID{ClientID: "workspace-writer", Seq: uint64(i + 1)}, delta)
		require.NoError(tb, err)
		ops = append(ops, op)
	}
	return ReplayPlan{
		EpochID:    11,
		Versions:   ReplayVersionRange{First: 1000, Count: uint64(count)},
		Operations: ops,
	}
}
