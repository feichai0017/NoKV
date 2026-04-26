package lsm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompactionPacerChargesByConfiguredRate(t *testing.T) {
	now := time.Unix(0, 0)
	var slept time.Duration
	p := newCompactionPacer(100)
	p.now = func() time.Time { return now }
	p.sleep = func(d time.Duration) {
		slept += d
		now = now.Add(d)
	}
	p.last = now

	p.charge(250)

	require.Equal(t, int64(250), p.charged.Load())
	require.Equal(t, 2500*time.Millisecond, slept)
	require.Equal(t, slept.Nanoseconds(), p.sleptNs.Load())
}

func TestCompactionPacerBypassesWhenL0IsNearStall(t *testing.T) {
	lm := &levelManager{
		opt: &Options{
			CompactionWriteBytesPerSec: 100,
			CompactionPacingBypassL0:   2,
		},
		compactionPacer: newCompactionPacer(100),
		levels: []*levelHandler{
			{tables: []*table{{}, {}}},
		},
	}

	require.True(t, lm.compactionPacerBypassActive())
	require.Nil(t, lm.compactionPacerForBuild())
}
