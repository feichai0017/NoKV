package capsule

import (
	"context"
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
	committer, err := NewBufferedCommitter(BufferedCommitterConfig{
		Holder:   holder,
		Snapshot: source,
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: noopInternalEntryApplier{},
	})
	require.NoError(t, err)

	_, err = committer.CommitCapsule(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.NoError(t, err)
	require.Equal(t, 1, holder.Pending())

	value, deleted, ok := committer.GetCapsuleOverlay([]byte("dentry/a"))
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
	versions := &fakeVersionAllocator{next: 100}
	committer, err := NewBufferedCommitter(BufferedCommitterConfig{
		Holder:   holder,
		Snapshot: source,
		Versions: versions,
		ReplayDB: noopInternalEntryApplier{},
	})
	require.NoError(t, err)

	_, err = committer.CommitCapsule(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.NoError(t, err)
	require.NoError(t, committer.Flush(context.Background()))

	_, _, ok := committer.GetCapsuleOverlay([]byte("dentry/a"))
	require.False(t, ok)
	require.Zero(t, holder.Pending())
	require.Equal(t, []uint64{1}, versions.counts)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["commit_total"])
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["apply_total"])
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
		if _, err := committer.CommitCapsule(ctx, id, deltaWithValueWrites("dentry/a", "inode=7")); err != nil {
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
