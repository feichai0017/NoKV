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

	ack, err := holder.Submit(context.Background(), opID("client-a", 1), opWithValueWrites("a", "v1"))
	require.NoError(t, err)

	require.Equal(t, uint64(1), ack.EpochID)
	require.Equal(t, opID("client-a", 1), ack.OpID)
	require.Equal(t, "holder-a", ack.HolderID)
	require.Equal(t, 1, holder.Pending())
	require.Empty(t, witness.snapshot().Segments)
}

func TestHolderSubmitReturnsPendingAckForSameOperationID(t *testing.T) {
	holder := newTestHolder(t)
	id := opID("client-a", 1)
	delta := opWithValueWrites("a", "v1")

	first, err := holder.Submit(context.Background(), id, delta)
	require.NoError(t, err)
	second, err := holder.Submit(context.Background(), id, delta)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Equal(t, 1, holder.Pending())
	plan, _, err := holder.BuildPendingReplayPlan(100)
	require.NoError(t, err)
	require.Equal(t, []OperationID{id}, []OperationID{plan.Operations[0].OpID})
}

func TestHolderSubmitRejectsSameOperationIDDifferentEffects(t *testing.T) {
	holder := newTestHolder(t)
	id := opID("client-a", 1)

	_, err := holder.Submit(context.Background(), id, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), id, opWithValueWrites("a", "v2"))
	require.ErrorIs(t, err, ErrDuplicateOperation)
	require.Equal(t, 1, holder.Pending())
}

func TestHolderBuildPendingReplayPlanUsesAdmissionOrder(t *testing.T) {
	holder := newTestHolder(t)
	first := opID("client-a", 1)
	second := opID("client-b", 1)

	_, err := holder.Submit(context.Background(), first, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, opWithValueWrites("a", "v2"))
	require.NoError(t, err)

	plan, _, err := holder.BuildPendingReplayPlan(100)
	require.NoError(t, err)
	require.Equal(t, []OperationID{first, second}, []OperationID{plan.Operations[0].OpID, plan.Operations[1].OpID})
	holder.MarkAppliedIDs(first)
	require.Equal(t, 1, holder.Pending())
}

func TestHolderBuildPendingReplayPlanLimitKeepsLaterOperationsPending(t *testing.T) {
	holder := newTestHolder(t)
	first := opID("client-a", 1)
	second := opID("client-b", 1)
	third := opID("client-c", 1)

	_, err := holder.Submit(context.Background(), first, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, opWithValueWrites("b", "v2"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), third, opWithValueWrites("c", "v3"))
	require.NoError(t, err)

	plan, _, err := holder.BuildPendingReplayPlanLimit(100, 2)
	require.NoError(t, err)
	require.Equal(t, []OperationID{first, second}, []OperationID{plan.Operations[0].OpID, plan.Operations[1].OpID})
	require.Equal(t, ReplayVersionRange{First: 100, Count: 2}, plan.Versions)

	require.NoError(t, holder.MarkReplayPlanApplied(plan))
	require.Equal(t, 1, holder.Pending())
	remaining := holder.PendingIDs()
	require.Equal(t, []OperationID{third}, remaining)
}

func TestHolderBuildPendingReplayPlanForScopeFiltersDisjointAuthority(t *testing.T) {
	holder := newTestHolder(t)
	firstScope := compile.AuthorityScope{
		Mount:           "vol",
		MountKeyID:      1,
		Parents:         []fsmeta.InodeID{1},
		Inodes:          []fsmeta.InodeID{10},
		AllowOpaqueKeys: true,
	}
	secondScope := compile.AuthorityScope{
		Mount:           "vol",
		MountKeyID:      1,
		Parents:         []fsmeta.InodeID{2},
		Inodes:          []fsmeta.InodeID{20},
		AllowOpaqueKeys: true,
	}
	first := opID("client-a", 1)
	second := opID("client-b", 1)
	firstDelta := deltaWithValueWrites("dentry/a", "v1")
	firstDelta.Authority = firstScope
	secondDelta := deltaWithValueWrites("dentry/b", "v2")
	secondDelta.Authority = secondScope

	_, err := holder.Submit(context.Background(), first, compile.MaterializeDelta(firstDelta, nil))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, compile.MaterializeDelta(secondDelta, nil))
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
	_, err = holder.Submit(context.Background(), first, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, err = holder.Submit(context.Background(), second, opWithValueWrites("a", "v2"))
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

	_, err := holder.Submit(context.Background(), opID("client-a", 1), compile.MaterializeDelta(delta, nil))
	require.ErrorIs(t, err, ErrIneligibleOperation)
}

func TestHolderAcceptsCrossBucketDelta(t *testing.T) {
	holder := newTestHolder(t)
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftKey := fsmetaInodeKeyForBucket(t, mount, 1)
	rightKey := fsmetaInodeKeyForBucket(t, mount, 2)
	delta := compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Eligibility: compile.EligibilityVisibleCommit,
		Authority: compile.AuthorityScope{
			Mount:      mount.MountID,
			MountKeyID: mount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{1, 2},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: leftKey, Value: []byte("left")},
			{Kind: compile.EffectPut, Key: rightKey, Value: []byte("right")},
		},
	}

	_, err := holder.Submit(context.Background(), opID("client-a", 1), compile.MaterializeDelta(delta, nil))
	require.NoError(t, err)
	require.Equal(t, 1, holder.Pending())
}

func BenchmarkHolderSubmitDisjoint(b *testing.B) {
	holder := mustHolderForBench(b)
	ctx := context.Background()
	delta := opWithValueWrites("bench-key", "value")

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

func opWithValueWrites(key, value string) compile.MaterializedOp {
	return compile.MaterializeDelta(deltaWithValueWrites(key, value), nil)
}

func deltaWithValueWrites(key, value string) compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Eligibility: compile.EligibilityVisibleCommit,
		Authority: compile.AuthorityScope{
			AllowOpaqueKeys: true,
		},
		WriteEffects: []compile.WriteEffect{{
			Kind:  compile.EffectPut,
			Key:   []byte(key),
			Value: []byte(value),
		}},
	}
}

func fsmetaInodeKeyForBucket(t *testing.T, mount fsmeta.MountIdentity, bucket fsmeta.AffinityBucket) []byte {
	t.Helper()
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) != bucket {
			continue
		}
		key, err := fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		return key
	}
	t.Fatalf("no inode found for bucket %d", bucket)
	return nil
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
