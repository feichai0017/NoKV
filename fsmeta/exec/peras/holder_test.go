package peras

import (
	"context"
	"errors"
	"sync"
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
	require.Len(t, commit.QuorumAckSet, 2)
	require.Subset(t, []string{"store-1", "store-2", "store-3"}, commit.QuorumAckSet)
	require.Equal(t, 1, holder.Pending())
	for _, replicaID := range commit.QuorumAckSet {
		replica := fakeWitnessByID(t, replicas, replicaID)
		snapshot := replica.snapshot()
		require.Len(t, snapshot.Prepares, 1)
		require.Len(t, snapshot.Commits, 1)
		require.Equal(t, snapshot.Prepares[0].OpID, snapshot.Commits[0].OpID)
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
		require.Empty(t, replica.snapshot().Commits)
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
	holder, err := NewHolder(HolderConfig{
		EpochID:   1,
		HolderID:  "holder-a",
		Witnesses: []WitnessReplica{replicas[0], replicas[1], replicas[2]},
		Quorum:    3,
		Now: func() time.Time {
			return time.Unix(10, 0)
		},
	})
	require.NoError(t, err)

	commit, err := holder.Submit(context.Background(), opID("client-a", 1), deltaWithWrites("a"))
	require.ErrorIs(t, err, ErrWitnessCommitAmbiguous)
	require.Equal(t, opID("client-a", 1), commit.OpID)
	require.Equal(t, 1, holder.Pending(), "partial commit evidence must keep the op fenced until recovery or seal")
}

func TestHolderSubmitReturnsAfterPrepareAndCommitQuorum(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	replicas[2].prepareDelay = time.Second
	replicas[2].commitDelay = time.Second
	holder := newTestHolder(t, replicas)

	start := time.Now()
	commit, err := holder.Submit(context.Background(), opID("client-a", 1), deltaWithWrites("a"))
	require.NoError(t, err)
	require.Less(t, time.Since(start), 200*time.Millisecond)
	require.Equal(t, []string{"store-1", "store-2"}, commit.QuorumAckSet)
}

func TestHolderSubmitDependencyFrontier(t *testing.T) {
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

	snapshot := replicas[0].snapshot()
	require.Len(t, snapshot.Prepares, 2)
	require.Equal(t, []OperationID{first}, snapshot.Prepares[1].DependencyFrontier)
	holder.MarkSealed(first)
	require.Equal(t, 1, holder.Pending())
}

func TestHolderBuildSealAndMarkApplied(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	holder, err := NewHolder(HolderConfig{
		EpochID:   1,
		HolderID:  "holder-a",
		Witnesses: []WitnessReplica{replicas[0], replicas[1], replicas[2]},
		Quorum:    3,
		Now: func() time.Time {
			return time.Unix(10, 0)
		},
	})
	require.NoError(t, err)

	first := opID("client-a", 1)
	second := opID("client-b", 1)
	_, err = holder.Submit(context.Background(), first, deltaWithWrites("a"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, deltaWithWrites("a"))
	require.NoError(t, err)
	require.Equal(t, 2, holder.Pending())

	seal, err := holder.BuildSeal(replicas[0].snapshot())
	require.NoError(t, err)
	require.Equal(t, uint64(1), seal.EpochID)
	require.Equal(t, 2, len(seal.Certificates))
	require.Equal(t, 2, holder.Pending(), "building a seal must not release the fence before raft apply")

	require.NoError(t, holder.MarkSealApplied(seal))
	require.Zero(t, holder.Pending())
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

func BenchmarkHolderBuildSeal64(b *testing.B) {
	holder := mustHolderForBench(b)
	snapshot := sealSnapshotForBench(b, 64)

	b.ReportAllocs()
	for b.Loop() {
		seal, err := holder.BuildSeal(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		if len(seal.Certificates) != 64 {
			b.Fatalf("unexpected cert count %d", len(seal.Certificates))
		}
	}
}

type fakeWitnessReplica struct {
	mu           sync.Mutex
	id           string
	prepareErr   error
	commitErr    error
	prepareDelay time.Duration
	commitDelay  time.Duration
	prepares     []PrepareRecord
	commits      []CommitCertificateRecord
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

func (r *fakeWitnessReplica) AppendPrepare(ctx context.Context, _ compile.AuthorityScope, record PrepareRecord) error {
	if err := waitFakeWitnessDelay(ctx, r.prepareDelay); err != nil {
		return err
	}
	if r.prepareErr != nil {
		return r.prepareErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prepares = append(r.prepares, clonePrepareRecord(record))
	return nil
}

func (r *fakeWitnessReplica) AppendCommitCertificate(ctx context.Context, _ compile.AuthorityScope, record CommitCertificateRecord) error {
	if err := waitFakeWitnessDelay(ctx, r.commitDelay); err != nil {
		return err
	}
	if r.commitErr != nil {
		return r.commitErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, cloneCommitCertificateRecord(record))
	return nil
}

func fakeWitnessByID(t *testing.T, replicas []*fakeWitnessReplica, id string) *fakeWitnessReplica {
	t.Helper()
	for _, replica := range replicas {
		if replica.id == id {
			return replica
		}
	}
	t.Fatalf("missing replica %q", id)
	return nil
}

func waitFakeWitnessDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *fakeWitnessReplica) snapshot() WitnessSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	prepares := make([]PrepareRecord, 0, len(r.prepares))
	for _, prepare := range r.prepares {
		prepares = append(prepares, clonePrepareRecord(prepare))
	}
	commits := make([]CommitCertificateRecord, 0, len(r.commits))
	for _, commit := range r.commits {
		commits = append(commits, cloneCommitCertificateRecord(commit))
	}
	return WitnessSnapshot{Prepares: prepares, Commits: commits}
}

func (r ackWitnessReplica) ID() string {
	return r.id
}

func (ackWitnessReplica) AppendPrepare(context.Context, compile.AuthorityScope, PrepareRecord) error {
	return nil
}

func (ackWitnessReplica) AppendCommitCertificate(context.Context, compile.AuthorityScope, CommitCertificateRecord) error {
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
