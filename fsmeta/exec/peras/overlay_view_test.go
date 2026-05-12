package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestOverlayViewGetScanFactsAndRemove(t *testing.T) {
	view := NewOverlayView()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, 9, "a")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	delta := compile.SemanticDelta{
		Kind: fsmeta.OperationCreate,
		ReadPredicates: []compile.Predicate{
			{Kind: compile.PredicateNotExists, Key: dentryKey},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: dentryKey, Value: []byte("dentry")},
			{Kind: compile.EffectPut, Key: inodeKey, Value: []byte("inode")},
		},
	}
	opID := OperationID{ClientID: "c", Seq: 1}
	require.NoError(t, view.Add(opID, delta))

	value, deleted, ok := view.Get(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry"), value)
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
