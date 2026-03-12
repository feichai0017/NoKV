package compact

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPolicy(t *testing.T) {
	require.Equal(t, PolicyLeveled, NewPolicy("").Name())
	require.Equal(t, PolicyLeveled, NewPolicy("unknown").Name())
	require.Equal(t, PolicyLeveled, NewPolicy(PolicyLeveled).Name())
	require.Equal(t, PolicyTiered, NewPolicy(PolicyTiered).Name())
	require.Equal(t, PolicyHybrid, NewPolicy(PolicyHybrid).Name())
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

