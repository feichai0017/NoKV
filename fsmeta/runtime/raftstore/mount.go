// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// MountResolver admits fsmeta operations against coordinator-rooted mount
// lifecycle state. V1 keeps one authority per mount; subtree handoff is still
// rooted but not split by this runtime.
type MountResolver struct {
	coordinator CoordinatorClient
}

func NewMountResolver(coordinator CoordinatorClient) (*MountResolver, error) {
	if coordinator == nil {
		return nil, errCoordinatorRequired
	}
	return &MountResolver{coordinator: coordinator}, nil
}

func (r *MountResolver) ResolveMount(ctx context.Context, mount model.MountID) (fsmetaexec.MountAdmission, error) {
	if r == nil || r.coordinator == nil {
		return fsmetaexec.MountAdmission{}, errCoordinatorRequired
	}
	resp, err := r.coordinator.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return fsmetaexec.MountAdmission{}, err
	}
	if resp.GetNotFound() || resp.GetMount() == nil {
		return fsmetaexec.MountAdmission{}, model.ErrMountNotRegistered
	}
	info := resp.GetMount()
	admission := fsmetaexec.MountAdmission{
		MountID:       model.MountID(info.GetMountId()),
		MountKeyID:    model.MountKeyID(info.GetMountKeyId()),
		RootInode:     model.InodeID(info.GetRootInode()),
		SchemaVersion: info.GetSchemaVersion(),
		Retired:       info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED,
	}
	if admission.MountID == "" || admission.MountKeyID == 0 {
		return fsmetaexec.MountAdmission{}, model.ErrMountNotRegistered
	}
	if admission.RootInode == 0 {
		admission.RootInode = model.RootInode
	}
	if admission.Retired {
		return fsmetaexec.MountAdmission{}, model.ErrMountRetired
	}
	return admission, nil
}

func (r *MountResolver) SameAuthority(ctx context.Context, mount model.MountID, _, _ model.InodeID) (bool, error) {
	_, err := r.ResolveMount(ctx, mount)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *MountResolver) StartSubtreeHandoff(ctx context.Context, mount model.MountID, _ model.InodeID, _ uint64) error {
	_, err := r.ResolveMount(ctx, mount)
	return err
}

func (r *MountResolver) CompleteSubtreeHandoff(ctx context.Context, mount model.MountID, _ model.InodeID, _ uint64) error {
	_, err := r.ResolveMount(ctx, mount)
	return err
}
