// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"testing"

	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
)

func TestApplyWatchObserverSplitsLargeApplyEvents(t *testing.T) {
	observer := newApplyWatchObserver(nil)
	keys := make([][]byte, defaultApplyWatchMaxKeysPerMessage+7)
	for i := range keys {
		keys[i] = []byte{byte(i)}
	}

	observer.OnApply(storepkg.ApplyEvent{
		RegionID:      7,
		Term:          2,
		Index:         11,
		Source:        storepkg.ApplyEventSourceCommit,
		CommitVersion: 99,
		Keys:          keys,
	})

	first := <-observer.ch
	second := <-observer.ch
	require.Len(t, first.GetKeys(), defaultApplyWatchMaxKeysPerMessage)
	require.Len(t, second.GetKeys(), 7)
	require.Equal(t, first.GetRegionId(), second.GetRegionId())
	require.Equal(t, first.GetTerm(), second.GetTerm())
	require.Equal(t, first.GetIndex(), second.GetIndex())
	require.Equal(t, first.GetCommitVersion(), second.GetCommitVersion())
}

func TestChunkApplyWatchKeysSplitsByApproximateKeyBytes(t *testing.T) {
	large := make([]byte, defaultApplyWatchMaxKeyBytesPerMessage/2+1)
	keys := [][]byte{large, large, []byte("tail")}

	chunks := chunkApplyWatchKeys(keys)

	require.Len(t, chunks, 2)
	require.Len(t, chunks[0], 1)
	require.Len(t, chunks[1], 2)
}
