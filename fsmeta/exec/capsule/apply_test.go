package capsule

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyReplayPlanAppliesOperationsInSealOrder(t *testing.T) {
	plan := replayPlanForTest(t)
	store := &recordingReplayStore{}

	stats, err := ApplyReplayPlan(store, plan)
	require.NoError(t, err)

	require.Equal(t, ApplyStats{Operations: 3, Mutations: 6}, stats)
	require.Equal(t, []OperationID{
		opID("client-a", 1),
		opID("client-c", 1),
		opID("client-b", 1),
	}, store.opIDs())
}

func TestApplyReplayPlanStopsOnStoreError(t *testing.T) {
	plan := replayPlanForTest(t)
	storeErr := errors.New("store failed")
	store := &recordingReplayStore{err: storeErr}

	stats, err := ApplyReplayPlan(store, plan)
	require.ErrorIs(t, err, storeErr)

	require.Equal(t, ApplyStats{}, stats)
	require.Empty(t, store.ops)
}

func TestApplyReplayPlanRejectsInvalidPlanBeforeApply(t *testing.T) {
	plan := replayPlanForTest(t)
	plan.Operations[1].Mutations[0].Key = nil
	store := &recordingReplayStore{}

	_, err := ApplyReplayPlan(store, plan)
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)
	require.Empty(t, store.ops)
}

func TestApplyReplayPlanRejectsNilStore(t *testing.T) {
	_, err := ApplyReplayPlan(nil, replayPlanForTest(t))
	require.ErrorIs(t, err, ErrReplayStoreRequired)
}

func TestApplyReplayPlanRejectsDuplicateOperations(t *testing.T) {
	plan := replayPlanForTest(t)
	plan.Operations[1].OpID = plan.Operations[0].OpID

	_, err := ApplyReplayPlan(&recordingReplayStore{}, plan)
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)
}

func TestApplyReplayPlanClonesStoreInput(t *testing.T) {
	plan := replayPlanForTest(t)
	store := &recordingReplayStore{mutateInput: true}

	_, err := ApplyReplayPlan(store, plan)
	require.NoError(t, err)

	require.Equal(t, []byte("dentry/a"), plan.Operations[0].Mutations[0].Key)
	require.Equal(t, []byte("inode=7"), plan.Operations[0].Mutations[0].Value)
}

func BenchmarkApplyReplayPlan64(b *testing.B) {
	seal, err := BuildCapsuleSeal(1, sealSnapshotForBench(b, 64))
	if err != nil {
		b.Fatal(err)
	}
	plan, err := BuildReplayPlan(seal)
	if err != nil {
		b.Fatal(err)
	}
	store := noopReplayStore{}

	b.ReportAllocs()
	for b.Loop() {
		stats, err := ApplyReplayPlan(store, plan)
		if err != nil {
			b.Fatal(err)
		}
		if stats.Operations != 64 {
			b.Fatalf("unexpected operation count %d", stats.Operations)
		}
	}
}

type recordingReplayStore struct {
	err         error
	mutateInput bool
	ops         []ReplayOperation
}

func (s *recordingReplayStore) ApplyCapsuleReplay(ops []ReplayOperation) error {
	if s.err != nil {
		return s.err
	}
	if s.mutateInput && len(ops) > 0 && len(ops[0].Mutations) > 0 {
		ops[0].Mutations[0].Key[0] = 'X'
		if len(ops[0].Mutations[0].Value) > 0 {
			ops[0].Mutations[0].Value[0] = 'Y'
		}
	}
	s.ops = append(s.ops, cloneReplayOperations(ops)...)
	return nil
}

func (s *recordingReplayStore) opIDs() []OperationID {
	out := make([]OperationID, 0, len(s.ops))
	for _, op := range s.ops {
		out = append(out, op.OpID)
	}
	return out
}

type noopReplayStore struct{}

func (noopReplayStore) ApplyCapsuleReplay([]ReplayOperation) error {
	return nil
}

func replayPlanForTest(t *testing.T) ReplayPlan {
	t.Helper()
	first := testSealPrepare()
	first.OpID = opID("client-a", 1)
	second := testSealPrepare()
	second.OpID = opID("client-b", 1)
	second.DependencyFrontier = []OperationID{first.OpID}
	third := testSealPrepare()
	third.OpID = opID("client-c", 1)

	seal, err := BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{second, third, first},
		Commits: []CommitCertificateRecord{
			testCommitForPrepare(t, second),
			testCommitForPrepare(t, third),
			testCommitForPrepare(t, first),
		},
	})
	require.NoError(t, err)
	plan, err := BuildReplayPlan(seal)
	require.NoError(t, err)
	return plan
}
