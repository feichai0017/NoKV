package capsule

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyReplayPlanAppliesWavesInOrder(t *testing.T) {
	plan := replayPlanForTest(t)
	store := &recordingReplayStore{}

	stats, err := ApplyReplayPlan(store, plan)
	require.NoError(t, err)

	require.Equal(t, ApplyStats{Waves: 2, Operations: 3, Mutations: 6}, stats)
	require.Equal(t, [][]OperationID{
		{opID("client-a", 1), opID("client-c", 1)},
		{opID("client-b", 1)},
	}, store.opIDs())
}

func TestApplyReplayPlanStopsOnWaveError(t *testing.T) {
	plan := replayPlanForTest(t)
	storeErr := errors.New("store failed")
	store := &recordingReplayStore{failAtWave: 1, err: storeErr}

	stats, err := ApplyReplayPlan(store, plan)
	require.ErrorIs(t, err, storeErr)

	require.Equal(t, ApplyStats{Waves: 1, Operations: 2, Mutations: 4}, stats)
	require.Equal(t, [][]OperationID{{opID("client-a", 1), opID("client-c", 1)}}, store.opIDs())
}

func TestApplyReplayPlanRejectsInvalidPlanBeforeApply(t *testing.T) {
	plan := replayPlanForTest(t)
	plan.Waves[1][0].Mutations[0].Key = nil
	store := &recordingReplayStore{}

	_, err := ApplyReplayPlan(store, plan)
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)
	require.Empty(t, store.waves)
}

func TestApplyReplayPlanRejectsNilStore(t *testing.T) {
	_, err := ApplyReplayPlan(nil, replayPlanForTest(t))
	require.ErrorIs(t, err, ErrReplayStoreRequired)
}

func TestApplyReplayPlanRejectsDuplicateOperations(t *testing.T) {
	plan := replayPlanForTest(t)
	plan.Waves[1][0].OpID = plan.Waves[0][0].OpID

	_, err := ApplyReplayPlan(&recordingReplayStore{}, plan)
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)
}

func TestApplyReplayPlanClonesStoreInput(t *testing.T) {
	plan := replayPlanForTest(t)
	store := &recordingReplayStore{mutateInput: true}

	_, err := ApplyReplayPlan(store, plan)
	require.NoError(t, err)

	require.Equal(t, []byte("dentry/a"), plan.Waves[0][0].Mutations[0].Key)
	require.Equal(t, []byte("inode=7"), plan.Waves[0][0].Mutations[0].Value)
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
	waves       [][]ReplayOperation
	failAtWave  int
	err         error
	mutateInput bool
}

func (s *recordingReplayStore) ApplyCapsuleReplayWave(wave []ReplayOperation) error {
	if s.err != nil && len(s.waves) == s.failAtWave {
		return s.err
	}
	if s.mutateInput && len(wave) > 0 && len(wave[0].Mutations) > 0 {
		wave[0].Mutations[0].Key[0] = 'X'
		if len(wave[0].Mutations[0].Value) > 0 {
			wave[0].Mutations[0].Value[0] = 'Y'
		}
	}
	s.waves = append(s.waves, cloneReplayWave(wave))
	return nil
}

func (s *recordingReplayStore) opIDs() [][]OperationID {
	out := make([][]OperationID, 0, len(s.waves))
	for _, wave := range s.waves {
		ids := make([]OperationID, 0, len(wave))
		for _, op := range wave {
			ids = append(ids, op.OpID)
		}
		out = append(out, ids)
	}
	return out
}

type noopReplayStore struct{}

func (noopReplayStore) ApplyCapsuleReplayWave([]ReplayOperation) error {
	return nil
}

func replayPlanForTest(t *testing.T) ReplayPlan {
	t.Helper()
	first := testSealPrepare()
	first.OpID = opID("client-a", 1)
	second := testSealPrepare()
	second.OpID = opID("client-b", 1)
	second.ConflictDAGFrontier = []OperationID{first.OpID}
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
