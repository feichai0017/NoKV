package capsule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestHolderSubmitRunsTwoPhaseWitnessCommit(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	holder := newTestHolder(t, replicas)

	commit, err := holder.Submit(context.Background(), opID("client-a", 1), deltaWithWrites("a"))
	require.NoError(t, err)

	require.Equal(t, uint64(1), commit.EpochID)
	require.Equal(t, opID("client-a", 1), commit.OpID)
	require.Equal(t, []string{"store-1", "store-2", "store-3"}, commit.QuorumAckSet)
	require.Equal(t, 1, holder.Pending())
	for _, replica := range replicas {
		require.Len(t, replica.prepares, 1)
		require.Len(t, replica.commits, 1)
		require.Equal(t, replica.prepares[0].OpID, replica.commits[0].OpID)
	}
}

func TestHolderSubmitRequiresPrepareQuorumBeforeCommit(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	replicas[1].prepareErr = errors.New("prepare failed")
	replicas[2].prepareErr = errors.New("prepare failed")
	holder := newTestHolder(t, replicas)

	_, err := holder.Submit(context.Background(), opID("client-a", 1), deltaWithWrites("a"))
	require.ErrorIs(t, err, ErrWitnessQuorumUnavailable)
	require.Zero(t, holder.Pending())
	for _, replica := range replicas {
		require.Empty(t, replica.commits)
	}
}

func TestHolderSubmitDetectsAmbiguousCommit(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	replicas[1].commitErr = errors.New("commit failed")
	replicas[2].commitErr = errors.New("commit failed")
	holder := newTestHolder(t, replicas)

	commit, err := holder.Submit(context.Background(), opID("client-a", 1), deltaWithWrites("a"))
	require.ErrorIs(t, err, ErrWitnessCommitAmbiguous)
	require.Equal(t, opID("client-a", 1), commit.OpID)
	require.Equal(t, 1, holder.Pending(), "partial commit evidence must keep the op fenced until recovery or seal")
}

func TestHolderSubmitConflictDAGFrontier(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	holder := newTestHolder(t, replicas)
	first := opID("client-a", 1)
	second := opID("client-b", 1)

	_, err := holder.Submit(context.Background(), first, deltaWithWrites("a"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, deltaWithWrites("a"))
	require.NoError(t, err)

	require.Len(t, replicas[0].prepares, 2)
	require.Equal(t, []OperationID{first}, replicas[0].prepares[1].ConflictDAGFrontier)
	holder.MarkSealed(first)
	require.Equal(t, 1, holder.Pending())
}

func TestHolderRejectsIneligibleOperation(t *testing.T) {
	holder := newTestHolder(t, []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	})
	delta := deltaWithWrites("a")
	delta.Eligibility = compile.EligibilitySlowPath

	_, err := holder.Submit(context.Background(), opID("client-a", 1), delta)
	require.ErrorIs(t, err, ErrIneligibleOperation)
}

func BenchmarkHolderSubmitDisjoint(b *testing.B) {
	holder := mustHolderForBench(b)
	ctx := context.Background()
	delta := deltaWithWrites("bench-key")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := OperationID{ClientID: "bench", Seq: uint64(i + 1)}
		if _, err := holder.Submit(ctx, id, delta); err != nil {
			b.Fatal(err)
		}
		holder.MarkSealed(id)
	}
}

type fakeWitnessReplica struct {
	id         string
	prepareErr error
	commitErr  error
	prepares   []PrepareRecord
	commits    []CommitCertificateRecord
}

type ackWitnessReplica struct {
	id string
}

func newFakeWitnessReplica(id string) *fakeWitnessReplica {
	return &fakeWitnessReplica{id: id}
}

func (r *fakeWitnessReplica) ID() string {
	return r.id
}

func (r *fakeWitnessReplica) AppendPrepare(_ context.Context, record PrepareRecord) error {
	if r.prepareErr != nil {
		return r.prepareErr
	}
	r.prepares = append(r.prepares, clonePrepareRecord(record))
	return nil
}

func (r *fakeWitnessReplica) AppendCommitCertificate(_ context.Context, record CommitCertificateRecord) error {
	if r.commitErr != nil {
		return r.commitErr
	}
	r.commits = append(r.commits, cloneCommitCertificateRecord(record))
	return nil
}

func (r ackWitnessReplica) ID() string {
	return r.id
}

func (ackWitnessReplica) AppendPrepare(context.Context, PrepareRecord) error {
	return nil
}

func (ackWitnessReplica) AppendCommitCertificate(context.Context, CommitCertificateRecord) error {
	return nil
}

func newTestHolder(t *testing.T, replicas []*fakeWitnessReplica) *Holder {
	t.Helper()
	witnesses := make([]WitnessReplica, 0, len(replicas))
	for _, replica := range replicas {
		witnesses = append(witnesses, replica)
	}
	holder, err := NewHolder(HolderConfig{
		EpochID:   1,
		HolderID:  "holder-a",
		Witnesses: witnesses,
		Now: func() time.Time {
			return time.Unix(10, 0)
		},
	})
	require.NoError(t, err)
	return holder
}

func mustHolderForBench(b *testing.B) *Holder {
	b.Helper()
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
		Witnesses: []WitnessReplica{
			ackWitnessReplica{id: "store-1"},
			ackWitnessReplica{id: "store-2"},
			ackWitnessReplica{id: "store-3"},
		},
		Now: func() time.Time {
			return time.Unix(10, 0)
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return holder
}
