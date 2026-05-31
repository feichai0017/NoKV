// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// InodeAllocator assigns create inode ids from coordinator/root TSO-adjacent
// monotone ID grants so every fsmeta server can allocate without local state.
type InodeAllocator struct {
	coordinator CoordinatorClient
}

func NewInodeAllocator(coordinator CoordinatorClient) (*InodeAllocator, error) {
	if coordinator == nil {
		return nil, errCoordinatorRequired
	}
	return &InodeAllocator{coordinator: coordinator}, nil
}

func (a *InodeAllocator) AllocateCreateInode(ctx context.Context, _ model.MountIdentity, _ model.InodeID, _ string) (model.InodeID, error) {
	if a == nil || a.coordinator == nil {
		return 0, errCoordinatorRequired
	}
	resp, err := a.coordinator.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	if err != nil {
		return 0, err
	}
	return model.InodeID(resp.GetFirstId()), nil
}
