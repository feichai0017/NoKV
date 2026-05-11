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

func TestDirectCommitterSubmitsSealsAndApplies(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	holder := newTestHolder(t, replicas)
	holder.quorum = 3
	db := openPerasReplayDB(t)
	versions := &fakeVersionAllocator{next: 100}
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Snapshot: fakeWitnessSnapshotSource{replica: replicas[0]},
		Versions: versions,
		ReplayDB: db,
	})
	require.NoError(t, err)

	seal, err := committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), seal.EpochID)
	require.Equal(t, ReplayVersionRange{First: 100, Count: 1}, seal.Versions)
	require.Zero(t, holder.Pending())
	require.Equal(t, []uint64{1}, versions.counts)

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("dentry/a"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("inode=7"), value)
}

func TestDirectCommitterKeepsPendingOnApplyFailure(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	holder := newTestHolder(t, replicas)
	holder.quorum = 3
	applyErr := errors.New("apply failed")
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Snapshot: fakeWitnessSnapshotSource{replica: replicas[0]},
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: &failingInternalEntryApplier{err: applyErr},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, 1, holder.Pending(), "durable commit evidence must stay fenced until recovery or retry seal applies")
}

func TestDirectCommitterRequiresSnapshotToCoverPendingCommit(t *testing.T) {
	replicas := []*fakeWitnessReplica{
		newFakeWitnessReplica("store-1"),
		newFakeWitnessReplica("store-2"),
		newFakeWitnessReplica("store-3"),
	}
	replicas[0].commitErr = errors.New("local witness missed commit")
	holder := newTestHolder(t, replicas)
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Snapshot: fakeWitnessSnapshotSource{replica: replicas[0]},
		Versions: &fakeVersionAllocator{next: 100},
		ReplayDB: noopInternalEntryApplier{},
	})
	require.NoError(t, err)

	_, err = committer.CommitPeras(context.Background(), opID("client-a", 1), deltaWithValueWrites("dentry/a", "inode=7"))
	require.ErrorIs(t, err, ErrInvalidPerasSeal)
	require.Equal(t, 1, holder.Pending(), "quorum commit exists but this seal source cannot prove it")
}

func BenchmarkDirectCommitterSingleOperation(b *testing.B) {
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
	committer, err := NewDirectCommitter(DirectCommitterConfig{
		Holder:   holder,
		Snapshot: source,
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
		if _, err := committer.CommitPeras(ctx, OperationID{ClientID: "bench", Seq: uint64(i + 1)}, delta); err != nil {
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

type fakeWitnessSnapshotSource struct {
	replica *fakeWitnessReplica
}

func (s fakeWitnessSnapshotSource) Probe(context.Context, uint64) (WitnessSnapshot, error) {
	return s.replica.snapshot(), nil
}

type drainingWitnessReplica struct {
	id       string
	prepares []PrepareRecord
	commits  []CommitCertificateRecord
}

func newDrainingWitnessReplica(id string) *drainingWitnessReplica {
	return &drainingWitnessReplica{id: id}
}

func (r *drainingWitnessReplica) ID() string {
	return r.id
}

func (r *drainingWitnessReplica) AppendPrepare(_ context.Context, _ compile.AuthorityScope, record PrepareRecord) error {
	r.prepares = append(r.prepares, clonePrepareRecord(record))
	return nil
}

func (r *drainingWitnessReplica) AppendCommitCertificate(_ context.Context, _ compile.AuthorityScope, record CommitCertificateRecord) error {
	r.commits = append(r.commits, cloneCommitCertificateRecord(record))
	return nil
}

func (r *drainingWitnessReplica) Probe(_ context.Context, epochID uint64) (WitnessSnapshot, error) {
	snapshot := WitnessSnapshot{
		Prepares: make([]PrepareRecord, 0, len(r.prepares)),
		Commits:  make([]CommitCertificateRecord, 0, len(r.commits)),
	}
	for _, prepare := range r.prepares {
		if prepare.EpochID == epochID {
			snapshot.Prepares = append(snapshot.Prepares, clonePrepareRecord(prepare))
		}
	}
	for _, commit := range r.commits {
		if commit.EpochID == epochID {
			snapshot.Commits = append(snapshot.Commits, cloneCommitCertificateRecord(commit))
		}
	}
	r.prepares = nil
	r.commits = nil
	return snapshot, nil
}

func deltaWithValueWrites(key, value string) compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Eligibility: compile.EligibilityFastPath,
		WriteEffects: []compile.WriteEffect{{
			Kind:  compile.EffectPut,
			Key:   []byte(key),
			Value: []byte(value),
		}},
	}
}
