// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const defaultInodeIDGrantSize = 1024

// InodeAllocator assigns create inode ids from coordinator/root TSO-adjacent
// monotone ID grants so every fsmeta server can allocate without local state.
type InodeAllocator struct {
	coordinator CoordinatorClient
	grantSize   uint64

	mu       sync.Mutex
	nextID   uint64
	grantEnd uint64
}

func NewInodeAllocator(coordinator CoordinatorClient) (*InodeAllocator, error) {
	if coordinator == nil {
		return nil, errCoordinatorRequired
	}
	return &InodeAllocator{coordinator: coordinator, grantSize: defaultInodeIDGrantSize}, nil
}

func (a *InodeAllocator) AllocateCreateInode(ctx context.Context, _ model.MountIdentity, _ model.InodeID, _ string) (model.InodeID, error) {
	if a == nil || a.coordinator == nil {
		return 0, errCoordinatorRequired
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nextID < a.grantEnd {
		id := a.nextID
		a.nextID++
		return model.InodeID(id), nil
	}
	grantSize := a.grantSize
	if grantSize == 0 {
		grantSize = defaultInodeIDGrantSize
	}
	resp, err := a.coordinator.AllocID(ctx, &coordpb.AllocIDRequest{Count: grantSize})
	if err != nil {
		return 0, err
	}
	first := resp.GetFirstId()
	count := resp.GetCount()
	if first == 0 || count == 0 {
		return 0, nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: invalid coordinator inode id grant")
	}
	if count > ^uint64(0)-first {
		return 0, nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: coordinator inode id grant overflows uint64")
	}
	a.nextID = first + 1
	a.grantEnd = first + count
	return model.InodeID(first), nil
}
