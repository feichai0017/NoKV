package peras

import (
	"context"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestHolderSubmitReturnsVisibleAckWithoutWitnessIO(t *testing.T) {
	witness := newFakeWitnessReplica("store-1")
	holder := newTestHolder(t)

	ack, err := holder.Submit(context.Background(), opID("client-a", 1), deltaWithValueWrites("a", "v1"))
	require.NoError(t, err)

	require.Equal(t, uint64(1), ack.EpochID)
	require.Equal(t, opID("client-a", 1), ack.OpID)
	require.Equal(t, "holder-a", ack.HolderID)
	require.Equal(t, 1, holder.Pending())
	require.Empty(t, witness.snapshot().Segments)
}

func TestHolderBuildPendingReplayPlanUsesAdmissionOrder(t *testing.T) {
	holder := newTestHolder(t)
	first := opID("client-a", 1)
	second := opID("client-b", 1)

	_, err := holder.Submit(context.Background(), first, deltaWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, deltaWithValueWrites("a", "v2"))
	require.NoError(t, err)

	plan, _, err := holder.BuildPendingReplayPlan(100)
	require.NoError(t, err)
	require.Equal(t, []OperationID{first, second}, []OperationID{plan.Operations[0].OpID, plan.Operations[1].OpID})
	holder.MarkAppliedIDs(first)
	require.Equal(t, 1, holder.Pending())
}

func TestHolderBuildPendingReplayPlanForScopeFiltersDisjointAuthority(t *testing.T) {
	holder := newTestHolder(t)
	firstScope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{10},
	}
	secondScope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{2},
		Inodes:     []fsmeta.InodeID{20},
	}
	first := opID("client-a", 1)
	second := opID("client-b", 1)
	firstDelta := deltaWithValueWrites("dentry/a", "v1")
	firstDelta.Authority = firstScope
	secondDelta := deltaWithValueWrites("dentry/b", "v2")
	secondDelta.Authority = secondScope

	_, err := holder.Submit(context.Background(), first, firstDelta)
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, secondDelta)
	require.NoError(t, err)

	plan, scope, ok, err := holder.BuildPendingReplayPlanForScope(100, firstScope)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, firstScope, scope)
	require.Equal(t, []OperationID{first}, []OperationID{plan.Operations[0].OpID})
	require.Equal(t, ReplayVersionRange{First: 100, Count: 1}, plan.Versions)

	require.NoError(t, holder.MarkReplayPlanApplied(plan))
	require.Equal(t, 1, holder.Pending())

	_, _, ok, err = holder.BuildPendingReplayPlanForScope(0, compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{99},
	})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestHolderBuildReplayPlanAndMarkApplied(t *testing.T) {
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	require.NoError(t, err)

	first := opID("client-a", 1)
	second := opID("client-b", 1)
	_, err = holder.Submit(context.Background(), first, deltaWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, deltaWithValueWrites("a", "v2"))
	require.NoError(t, err)
	require.Equal(t, 2, holder.Pending())

	plan, _, err := holder.BuildPendingReplayPlan(100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), plan.EpochID)
	require.Equal(t, ReplayVersionRange{First: 100, Count: 2}, plan.Versions)
	require.Equal(t, 2, len(plan.Operations))
	require.Equal(t, 2, holder.Pending(), "building a segment plan must not release the fence before apply")

	require.NoError(t, holder.MarkReplayPlanApplied(plan))
	require.Zero(t, holder.Pending())
}

func TestHolderRejectsIneligibleOperation(t *testing.T) {
	holder := newTestHolder(t)
	delta := deltaWithValueWrites("a", "v1")
	delta.Eligibility = compile.EligibilitySlowPath

	_, err := holder.Submit(context.Background(), opID("client-a", 1), delta)
	require.ErrorIs(t, err, ErrIneligibleOperation)
}

func BenchmarkHolderSubmitDisjoint(b *testing.B) {
	holder := mustHolderForBench(b)
	ctx := context.Background()
	delta := deltaWithValueWrites("bench-key", "value")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := OperationID{ClientID: "bench", Seq: uint64(i + 1)}
		if _, err := holder.Submit(ctx, id, delta); err != nil {
			b.Fatal(err)
		}
		holder.MarkAppliedIDs(id)
	}
}

type fakeWitnessReplica struct {
	mu       sync.Mutex
	id       string
	segments []SegmentWitnessRecord
}

func newFakeWitnessReplica(id string) *fakeWitnessReplica {
	return &fakeWitnessReplica{id: id}
}

func (r *fakeWitnessReplica) ID() string {
	return r.id
}

func (r *fakeWitnessReplica) AppendSegment(_ context.Context, _ compile.AuthorityScope, record SegmentWitnessRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segments = append(r.segments, record)
	return nil
}

func (r *fakeWitnessReplica) Probe(context.Context, uint64) (WitnessSnapshot, error) {
	return r.snapshot(), nil
}

func (r *fakeWitnessReplica) snapshot() WitnessSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := make([]SegmentWitnessRecord, 0, len(r.segments))
	for _, segment := range r.segments {
		segments = append(segments, segment)
	}
	return WitnessSnapshot{Segments: segments}
}

func newTestHolder(t *testing.T) *Holder {
	t.Helper()
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	require.NoError(t, err)
	return holder
}

func mustHolderForBench(b *testing.B) *Holder {
	b.Helper()
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	if err != nil {
		b.Fatal(err)
	}
	return holder
}
