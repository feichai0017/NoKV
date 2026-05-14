package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestOverlayViewGetScanFactsAndRemove(t *testing.T) {
	view := NewOverlayView()
	dentryKey, err := fsmeta.EncodeDentryKey(testMount, 9, "a")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(testMount, 10)
	require.NoError(t, err)
	op := testGeneratedCreateOpForInodes(t, 9, 10, "a")
	opID := OperationID{ClientID: "c", Seq: 1}
	require.NoError(t, view.Add(opID, op))

	value, deleted, ok := view.Get(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, op.Effects[0].Value, value)
	present, known := view.KeyState(dentryKey)
	require.True(t, present)
	require.True(t, known)
	present, known = view.KeyState([]byte("missing"))
	require.False(t, present)
	require.False(t, known)
	require.Len(t, view.Scan(nil, 8), 2)

	plan := ReplayPlan{Operations: []ReplayOperation{{
		OpID: opID,
		Mutations: []ReplayMutation{
			{Key: dentryKey},
			{Key: inodeKey},
		},
	}}}
	view.RemovePlan(plan)
	_, _, ok = view.Get(dentryKey)
	require.False(t, ok)
	overlayKeys, knownKeys, _, _ := view.Stats()
	require.Zero(t, overlayKeys)
	require.NotZero(t, knownKeys)
}
