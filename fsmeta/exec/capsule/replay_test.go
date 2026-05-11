package capsule

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestBuildReplayPlanDecodesConcreteEffects(t *testing.T) {
	prepare := testSealPrepare()
	seal, err := BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{prepare},
		Commits:  []CommitCertificateRecord{testCommitForPrepare(t, prepare)},
	})
	require.NoError(t, err)

	plan, err := BuildReplayPlan(seal)
	require.NoError(t, err)

	require.Equal(t, uint64(1), plan.EpochID)
	require.Len(t, plan.Operations, 1)
	require.Equal(t, prepare.OpID, plan.Operations[0].OpID)
	require.Len(t, plan.Operations[0].Mutations, 2)
	require.False(t, plan.Operations[0].Mutations[0].Delete)
	require.Equal(t, []byte("dentry/a"), plan.Operations[0].Mutations[0].Key)
	require.Equal(t, []byte("inode=7"), plan.Operations[0].Mutations[0].Value)
}

func TestBuildReplayPlanUsesSealOrderWithoutReplayGrouping(t *testing.T) {
	first := testSealPrepare()
	first.OpID = OperationID{ClientID: "client-a", Seq: 1}
	second := testSealPrepare()
	second.OpID = OperationID{ClientID: "client-b", Seq: 1}
	second.DependencyFrontier = []OperationID{first.OpID}
	third := testSealPrepare()
	third.OpID = OperationID{ClientID: "client-c", Seq: 1}

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

	require.Equal(t, []OperationID{first.OpID, third.OpID, second.OpID}, []OperationID{
		plan.Operations[0].OpID,
		plan.Operations[1].OpID,
		plan.Operations[2].OpID,
	})
}

func TestBuildReplayPlanRejectsNonConcreteEffects(t *testing.T) {
	delta := testSemanticDelta()
	delta.WriteEffects[0].Kind = compile.EffectDerivedPut
	payload, err := EncodeSemanticDeltaPayload(delta)
	require.NoError(t, err)

	prepare := testSealPrepare()
	setPrepareDeltaPayload(&prepare, payload)
	seal, err := BuildCapsuleSeal(1, WitnessSnapshot{
		Prepares: []PrepareRecord{prepare},
		Commits:  []CommitCertificateRecord{testCommitForPrepare(t, prepare)},
	})
	require.NoError(t, err)

	_, err = BuildReplayPlan(seal)
	require.ErrorIs(t, err, ErrInvalidCapsuleSeal)
}

func BenchmarkBuildReplayPlan64(b *testing.B) {
	seal, err := BuildCapsuleSeal(1, sealSnapshotForBench(b, 64))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		plan, err := BuildReplayPlan(seal)
		if err != nil {
			b.Fatal(err)
		}
		if len(plan.Operations) == 0 {
			b.Fatal("empty plan")
		}
	}
}
