// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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

func TestOverlayViewScanReusesSortedIndexAcrossReads(t *testing.T) {
	view := NewOverlayView()
	for _, item := range []struct {
		name string
		seq  uint64
	}{
		{name: "c", seq: 1},
		{name: "a", seq: 2},
		{name: "b", seq: 3},
	} {
		op := testGeneratedCreateOpForInodes(t, 9, fsmeta.InodeID(20+item.seq), item.name)
		require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: item.seq}, op))
	}
	prefix, err := fsmeta.EncodeDentryPrefix(testMount, 9)
	require.NoError(t, err)

	first := view.Scan(prefix, 3)
	second := view.Scan(prefix, 3)
	require.Equal(t, first, second)
	require.Len(t, first, 3)
	require.Less(t, string(first[0].Key), string(first[1].Key))
	require.Less(t, string(first[1].Key), string(first[2].Key))
}
