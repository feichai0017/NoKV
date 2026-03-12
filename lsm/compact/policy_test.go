package compact

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPolicy(t *testing.T) {
	require.IsType(t, LeveledPolicy{}, NewPolicy(""))
	require.IsType(t, LeveledPolicy{}, NewPolicy("unknown"))
	require.IsType(t, LeveledPolicy{}, NewPolicy(PolicyLeveled))
	require.IsType(t, &TieredPolicy{}, NewPolicy(PolicyTiered))
	require.IsType(t, &HybridPolicy{}, NewPolicy(PolicyHybrid))
}

func TestLeveledPolicyArrange(t *testing.T) {
	p := LeveledPolicy{}
	in := []Priority{
		{Level: 1, Adjusted: 2},
		{Level: 0, Adjusted: 1},
		{Level: 2, Adjusted: 0.5},
	}

	forWorker0 := p.Arrange(0, in)
	require.Equal(t, 0, forWorker0[0].Level)
	require.Equal(t, 1, forWorker0[1].Level)

	forWorker1 := p.Arrange(1, in)
	require.Equal(t, 1, forWorker1[0].Level)
	require.Equal(t, 0, forWorker1[1].Level)
}

func TestTieredPolicyArrangePrefersIngest(t *testing.T) {
	p := &TieredPolicy{}
	in := []Priority{
		{Level: 0, Adjusted: 9, IngestMode: IngestNone},
		{Level: 3, Adjusted: 2, IngestMode: IngestKeep},
		{Level: 2, Adjusted: 5, IngestMode: IngestDrain},
		{Level: 1, Adjusted: 8, IngestMode: IngestNone},
	}
	out := p.Arrange(0, in)
	require.Len(t, out, 4)
	// Worker 0 should reserve one critical L0 slot before ingest tasks.
	require.Equal(t, 0, out[0].Level)
	require.Equal(t, IngestKeep, out[1].IngestMode)
	require.Equal(t, IngestDrain, out[2].IngestMode)
	// Regular progress is still preserved in the same cycle.
	require.Equal(t, 1, out[3].Level)
}

func TestHybridPolicyArrangeSwitchesByIngestPresence(t *testing.T) {
	p := &HybridPolicy{}
	// Mild ingest pressure keeps leveled ordering.
	withMildIngest := []Priority{
		{Level: 1, Adjusted: 2, IngestMode: IngestNone},
		{Level: 2, Adjusted: 1.5, IngestMode: IngestDrain},
	}
	out := p.Arrange(0, withMildIngest)
	require.Equal(t, 1, out[0].Level)

	noIngest := []Priority{
		{Level: 2, Adjusted: 2, IngestMode: IngestNone},
		{Level: 0, Adjusted: 1.5, IngestMode: IngestNone},
	}
	out = p.Arrange(0, noIngest)
	require.Equal(t, 0, out[0].Level)

	// High ingest pressure switches to tiered queue scheduling.
	withHeavyIngest := []Priority{
		{Level: 1, Adjusted: 1.2, IngestMode: IngestNone},
		{Level: 2, Adjusted: 4.5, IngestMode: IngestDrain},
		{Level: 3, Adjusted: 3.5, IngestMode: IngestKeep},
	}
	out = p.Arrange(0, withHeavyIngest)
	require.Equal(t, IngestKeep, out[0].IngestMode)
	require.Equal(t, IngestDrain, out[1].IngestMode)
}

func TestTieredPolicyFeedbackAdjustsQuota(t *testing.T) {
	baseInput := []Priority{
		{Level: 0, Adjusted: 3.0, IngestMode: IngestNone},
		{Level: 6, Adjusted: 6.0, IngestMode: IngestKeep},
		{Level: 6, Adjusted: 5.9, IngestMode: IngestKeep},
		{Level: 6, Adjusted: 5.8, IngestMode: IngestKeep},
		{Level: 6, Adjusted: 5.7, IngestMode: IngestKeep},
		{Level: 5, Adjusted: 6.5, IngestMode: IngestDrain},
		{Level: 5, Adjusted: 6.4, IngestMode: IngestDrain},
		{Level: 5, Adjusted: 6.3, IngestMode: IngestDrain},
		{Level: 5, Adjusted: 6.2, IngestMode: IngestDrain},
		{Level: 2, Adjusted: 5.5, IngestMode: IngestNone},
		{Level: 2, Adjusted: 5.4, IngestMode: IngestNone},
	}

	normal := &TieredPolicy{}
	normalOut := normal.Arrange(0, baseInput)
	normalIdx := firstRegularNonL0(normalOut)
	require.Greater(t, normalIdx, 0)

	failed := &TieredPolicy{}
	for range 3 {
		failed.Observe(FeedbackEvent{
			Priority: Priority{IngestMode: IngestDrain},
			Err:      errors.New("injected ingest failure"),
		})
	}
	failedOut := failed.Arrange(0, baseInput)
	failedIdx := firstRegularNonL0(failedOut)
	require.Less(t, failedIdx, normalIdx, "ingest failures should shift quota toward regular progress")

	success := &TieredPolicy{}
	for range 3 {
		success.Observe(FeedbackEvent{
			Priority: Priority{IngestMode: IngestKeep},
			Err:      nil,
		})
	}
	successOut := success.Arrange(0, baseInput)
	successIdx := firstRegularNonL0(successOut)
	require.Greater(t, successIdx, normalIdx, "ingest successes should increase ingest scheduling share")
}

func firstRegularNonL0(prios []Priority) int {
	for i, p := range prios {
		if p.IngestMode == IngestNone && p.Level != 0 {
			return i
		}
	}
	return -1
}
