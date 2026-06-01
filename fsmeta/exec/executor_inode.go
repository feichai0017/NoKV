// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// UpdateInode updates mutable inode attributes and applies the size quota delta
// in the same metadata command. The parent field is required because quota is
// directory-scoped by parent inode.
func (e *Executor) UpdateInode(ctx context.Context, req model.UpdateInodeRequest) (model.InodeRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.InodeRecord{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileUpdateInodeProgram(req, mount, compile.WithQuotaMode(e.quotaMode()))
	if err != nil {
		return model.InodeRecord{}, err
	}
	delta := program.Compiled.Delta
	plan := delta.Plan
	if !req.SetSize && !req.SetMode && !req.SetUpdatedUnixNs && !req.SetOpaqueAttrs {
		return model.InodeRecord{}, model.ErrInvalidRequest
	}
	var updated model.InodeRecord
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		dentry, err := e.readDentrySnapshot(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if dentry.record.Inode != req.Inode {
			return model.ErrInvalidRequest
		}
		inode, ok, err := e.readInode(ctx, mount, req.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return model.ErrNotFound
		}
		if dentry.record.Type != inode.Type {
			return model.ErrInvalidValue
		}
		// fsmeta does not maintain an inode->parents reverse index. Updating a
		// hard-linked inode would require invalidating and quota-adjusting every
		// parent, so reject it rather than silently corrupting accounting.
		if inode.LinkCount != 1 {
			return model.ErrInvalidRequest
		}
		oldInodeValue, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		sizeDelta := int64(0)
		if req.SetSize {
			sizeDelta = inodeSizeChange(inode.Size, req.Size)
			inode.Size = req.Size
		}
		if req.SetMode {
			inode.Mode = req.Mode
		}
		if req.SetUpdatedUnixNs {
			inode.UpdatedUnixNs = req.UpdatedUnixNs
		}
		if req.SetOpaqueAttrs {
			inode.OpaqueAttrs = append([]byte(nil), req.OpaqueAttrs...)
		}
		value, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		dentryValue, err := encodeDentryValueForCommit(dentry.record, inode, true, commitVersion)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{
			{
				Op:    backend.MutationPut,
				Key:   cloneBytes(plan.MutateKeys[0]),
				Value: value,
			},
		}
		var quotaMutations []*backend.Mutation
		if sizeDelta != 0 {
			quotaMutations, err = e.reserveQuota(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      sizeDelta,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		mutations = append(mutations, &backend.Mutation{
			Op:    backend.MutationPut,
			Key:   cloneBytes(plan.ReadKeys[0]),
			Value: dentryValue,
		})
		watchEvent, err := dentryUpdateWatchEvent(mount, req.Parent, req.Name, req.Inode)
		if err != nil {
			return err
		}
		if len(quotaMutations) == 0 {
			predicates := []*backend.Predicate{
				metadataValueEqualsPredicate(plan.ReadKeys[0], dentry.value),
				metadataValueEqualsPredicate(plan.MutateKeys[0], oldInodeValue),
			}
			if err := e.commitWithMetadataPredicatesAndWatch(ctx, plan.Kind, mount, plan.PrimaryKey, predicates, mutations, []backend.WatchEvent{watchEvent}, startVersion, commitVersion); err != nil {
				return err
			}
		} else if err := e.commitWithoutMetadataPredicatesAndWatch(ctx, plan.Kind, mount, plan.PrimaryKey, mutations, []backend.WatchEvent{watchEvent}, startVersion, commitVersion); err != nil {
			return err
		}
		updated = inode
		return nil
	}, delta.Authority); err != nil {
		return model.InodeRecord{}, err
	}
	return updated, nil
}
