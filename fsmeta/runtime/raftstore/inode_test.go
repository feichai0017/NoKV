// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestInodeAllocatorCachesCoordinatorGrant(t *testing.T) {
	coordinator := fakeRouteCoordinator()
	allocator, err := NewInodeAllocator(coordinator)
	require.NoError(t, err)
	allocator.grantSize = 4

	for want := uint64(100); want < 104; want++ {
		got, err := allocator.AllocateCreateInode(context.Background(), model.MountIdentity{}, 0, "file")
		require.NoError(t, err)
		require.Equal(t, want, uint64(got))
	}
	require.Equal(t, uint64(1), coordinator.allocCallCount())
	require.Equal(t, []uint64{4}, coordinator.allocRequestCounts())

	got, err := allocator.AllocateCreateInode(context.Background(), model.MountIdentity{}, 0, "file")
	require.NoError(t, err)
	require.Equal(t, uint64(104), uint64(got))
	require.Equal(t, uint64(2), coordinator.allocCallCount())
	require.Equal(t, []uint64{4, 4}, coordinator.allocRequestCounts())
}
