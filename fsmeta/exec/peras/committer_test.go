package peras

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/txn/percolator"
	"github.com/stretchr/testify/require"
)

func TestDirectCommitterSubmitsAndApplies(t *testing.T) {
	holder := newTestHolder(t)
	db := openPerasReplayDB(t)
	versions := &fakeVersionAllocator{next: 100}
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Versions: versions,
		ReplayDB: db,
	})
	require.NoError(t, err)

	ack, err := committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), ack.EpochID)
	require.Equal(t, opID("client-a", 1), ack.OpID)
	require.Zero(t, holder.Pending())
	require.Equal(t, []uint64{1}, versions.counts)

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("dentry/a"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("inode=7"), value)
}

func TestDirectCommitterKeepsPendingOnApplyFailure(t *testing.T) {
	holder := newTestHolder(t)
	applyErr := errors.New("apply failed")
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: &failingInternalEntryApplier{err: applyErr},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"), nil)
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, 1, holder.Pending(), "durable commit evidence must stay fenced until recovery or retry seal applies")
}

func TestDirectCommitterDoesNotRequireWitnessSnapshot(t *testing.T) {
	holder := newTestHolder(t)
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: noopInternalEntryApplier{},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"), nil)
	require.NoError(t, err)
	require.Zero(t, holder.Pending())
}

func BenchmarkDirectCommitterSingleOperation(b *testing.B) {
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	if err != nil {
		b.Fatal(err)
	}
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Versions: &fakeVersionAllocator{next: 1},
		ReplayDB: noopInternalEntryApplier{},
	})
	if err != nil {
		b.Fatal(err)
	}
	delta := deltaWithValueWrites("dentry/a", "inode=7")
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := committer.CommitPeras(ctx, OperationID{ClientID: "bench", Seq: uint64(i + 1)}, delta, nil); err != nil {
			b.Fatal(err)
		}
	}
}

type fakeVersionAllocator struct {
	next   uint64
	counts []uint64
	err    error
}

func (a *fakeVersionAllocator) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if a.err != nil {
		return 0, a.err
	}
	first := a.next
	a.next += count
	a.counts = append(a.counts, count)
	return first, nil
}

func deltaWithValueWrites(key, value string) compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Eligibility: compile.EligibilityVisibleCommit,
		WriteEffects: []compile.WriteEffect{{
			Kind:  compile.EffectPut,
			Key:   []byte(key),
			Value: []byte(value),
		}},
	}
}
