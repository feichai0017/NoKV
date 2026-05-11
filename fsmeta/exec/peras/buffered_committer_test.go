package peras

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferedCommitterReturnsBeforeSealAndServesOverlay(t *testing.T) {
	source := newDrainingWitnessReplica("store-1")
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
		Witnesses: []WitnessReplica{
			source,
			newDrainingWitnessReplica("store-2"),
			newDrainingWitnessReplica("store-3"),
		},
	})
	require.NoError(t, err)
	holder.quorum = 3
	committer, err := NewBufferedCommitter(BufferedCommitterConfig{
		Holder:   holder,
		Snapshot: source,
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: noopInternalEntryApplier{},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.NoError(t, err)
	require.Equal(t, 1, holder.Pending())

	value, deleted, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("inode=7"), value)
	require.Equal(t, 1, committer.Stats()["overlay_keys"])
}

func TestBufferedCommitterFlushAppliesAndClearsOverlay(t *testing.T) {
	source := newDrainingWitnessReplica("store-1")
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
		Witnesses: []WitnessReplica{
			source,
			newDrainingWitnessReplica("store-2"),
			newDrainingWitnessReplica("store-3"),
		},
	})
	require.NoError(t, err)
	holder.quorum = 3
	versions := &fakeVersionAllocator{next: 100}
	committer, err := NewBufferedCommitter(BufferedCommitterConfig{
		Holder:   holder,
		Snapshot: source,
		Versions: versions,
		ReplayDB: noopInternalEntryApplier{},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.NoError(t, err)
	require.NoError(t, committer.Flush(context.Background()))

	_, _, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.False(t, ok)
	require.Zero(t, holder.Pending())
	require.Equal(t, []uint64{1}, versions.counts)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["commit_total"])
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["apply_total"])
}

func TestBufferedCommitterFlushBuildsSegmentAndReportsStats(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	holder := newTestHolder(t, replicas)
	holder.quorum = 3
	var hookedSegment PerasSegment
	var hookedStats SegmentStats
	committer, err := NewBufferedCommitter(BufferedCommitterConfig{
		Holder:   holder,
		Snapshot: fakeWitnessSnapshotSource{replica: replicas[0]},
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: noopInternalEntryApplier{},
		SegmentHook: func(segment PerasSegment, stats SegmentStats) {
			hookedSegment = segment
			hookedStats = stats
		},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.NoError(t, err)
	_, err = committer.CommitPeras(context.Background(), opID("client-a", 2), deltaWithValueWrites("dentry/a", "inode=8"))
	require.NoError(t, err)
	require.NoError(t, committer.Flush(context.Background()))

	require.NotZero(t, hookedSegment.Root)
	require.Equal(t, uint64(2), hookedStats.OperationCount)
	require.Equal(t, uint64(2), hookedStats.InputMutationCount)
	require.Equal(t, uint64(1), hookedStats.EntryCount)
	require.Equal(t, uint64(1), hookedStats.CoalescedMutations)
	value, deleted, ok := hookedSegment.Get([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("inode=8"), value)

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, uint64(1), stats["segment_entries_total"])
	require.Equal(t, uint64(2), stats["last_segment_operations"])
	require.Equal(t, uint64(2), stats["last_segment_input_mutations"])
	require.Equal(t, uint64(1), stats["last_segment_entries"])
	require.Equal(t, uint64(1), stats["last_segment_coalesced"])
	require.Equal(t, uint64(200), stats["last_segment_compression_x100"])
	require.Equal(t, hookedSegment.Root, stats["last_segment_root"])
}

func TestBufferedCommitterDoesNotPublishSegmentOnApplyFailure(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	holder := newTestHolder(t, replicas)
	holder.quorum = 3
	applyErr := errors.New("apply failed")
	hookCalls := 0
	committer, err := NewBufferedCommitter(BufferedCommitterConfig{
		Holder:   holder,
		Snapshot: fakeWitnessSnapshotSource{replica: replicas[0]},
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: &failingInternalEntryApplier{err: applyErr},
		SegmentHook: func(PerasSegment, SegmentStats) {
			hookCalls++
		},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.NoError(t, err)
	err = committer.Flush(context.Background())
	require.ErrorIs(t, err, applyErr)

	require.Equal(t, 0, hookCalls)
	require.Equal(t, 1, holder.Pending())
	_, _, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.True(t, ok)
	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["segment_total"])
	require.Equal(t, uint64(0), stats["flush_total"])
	require.Equal(t, uint64(0), stats["apply_total"])
}

func BenchmarkBufferedCommitterHotPath(b *testing.B) {
	source := newDrainingWitnessReplica("store-1")
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
		Witnesses: []WitnessReplica{
			source,
			newDrainingWitnessReplica("store-2"),
			newDrainingWitnessReplica("store-3"),
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	holder.quorum = 3
	committer, err := NewBufferedCommitter(BufferedCommitterConfig{
		Holder:   holder,
		Snapshot: source,
		Versions: &fakeVersionAllocator{next: 1},
		ReplayDB: noopInternalEntryApplier{},
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := OperationID{ClientID: "bench", Seq: uint64(i + 1)}
		if _, err := committer.CommitPeras(ctx, id, deltaWithValueWrites("dentry/a", "inode=7")); err != nil {
			b.Fatal(err)
		}
		if i%64 == 63 {
			b.StopTimer()
			if err := committer.Flush(ctx); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
}
