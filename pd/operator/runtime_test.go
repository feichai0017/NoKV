package operator

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRuntimeAssignsAdmissionAndBackoff(t *testing.T) {
	rt := NewRuntime()
	now := time.Unix(100, 0)
	rt.clock = func() time.Time { return now }

	rt.ReplaceRootedTransitions([]rootstate.TransitionEntry{
		{
			Kind:       rootstate.TransitionKindPeerChange,
			Key:        10,
			Status:     rootstate.TransitionStatusConflict,
			RetryClass: rootstate.TransitionRetryConflict,
		},
	})

	snap := rt.Snapshot()
	require.Len(t, snap.Entries, 1)
	require.Equal(t, defaultOwner, snap.Entries[0].Owner)
	require.Equal(t, uint64(1), snap.Entries[0].Attempt)
	require.False(t, snap.Entries[0].Admitted)
	require.Equal(t, now.Add(defaultConflictBackoff), snap.Entries[0].BackoffUntil)
}

func TestRuntimePreservesExistingOperatorState(t *testing.T) {
	rt := NewRuntime()
	first := time.Unix(100, 0)
	rt.clock = func() time.Time { return first }

	entry := rootstate.TransitionEntry{
		Kind:   rootstate.TransitionKindPeerChange,
		Key:    20,
		Status: rootstate.TransitionStatusPending,
	}
	rt.ReplaceRootedTransitions([]rootstate.TransitionEntry{entry})

	second := time.Unix(200, 0)
	rt.clock = func() time.Time { return second }
	rt.ReplaceRootedTransitions([]rootstate.TransitionEntry{entry})

	snap := rt.Snapshot()
	require.Len(t, snap.Entries, 1)
	require.Equal(t, uint64(1), snap.Entries[0].Attempt)
	require.Equal(t, defaultOwner, snap.Entries[0].Owner)
	require.True(t, snap.Entries[0].Admitted)
	require.True(t, snap.Entries[0].BackoffUntil.IsZero())
}
