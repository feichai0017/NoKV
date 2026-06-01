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

type removeDentryRequest struct {
	Mount  model.MountID
	Parent model.InodeID
	Name   string
}

func (e *Executor) removeDentry(ctx context.Context, mount model.MountIdentity, compiled compile.CompiledOp, req removeDentryRequest) (model.RemoveResult, error) {
	delta := compiled.Delta
	plan := delta.Plan
	var result model.RemoveResult
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		dentry, err := e.readDentrySnapshot(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		record := dentry.record
		watchEvent, err := dentryWatchEvent(mount, backend.WatchOperationDelete, record)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{{
			Op:  backend.MutationDelete,
			Key: cloneBytes(plan.MutateKeys[0]),
		}}
		parentLinkDelete, err := parentIndexDeleteMutation(mount, record)
		if err != nil {
			return err
		}
		mutations = append(mutations, parentLinkDelete)
		attemptResult := model.RemoveResult{RemovedDentry: record}
		predicates := []*backend.Predicate{metadataValueEqualsPredicate(plan.PrimaryKey, dentry.value)}
		var quotaMutations []*backend.Mutation
		parent, err := e.readDirectoryInode(ctx, mount, req.Parent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := decrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := layout.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		predicates = append(predicates, metadataValueEqualsPredicate(parent.key, parent.value))
		if inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion); err != nil {
			return err
		} else if ok {
			attemptResult.OldInode = inode
			inodeKey, err := layout.EncodeInodeKey(mount, inode.Inode)
			if err != nil {
				return err
			}
			if inode.Type == model.InodeTypeDirectory {
				return model.ErrInvalidRequest
			}
			oldInodeValue, err := layout.EncodeInodeValue(inode)
			if err != nil {
				return err
			}
			predicates = append(predicates, metadataValueEqualsPredicate(inodeKey, oldInodeValue))
			if inode.LinkCount <= 1 {
				attemptResult.InodeDeleted = true
				mutations = append(mutations, &backend.Mutation{Op: backend.MutationDelete, Key: inodeKey})
			} else {
				inode.LinkCount--
				inodeValue, err := layout.EncodeInodeValue(inode)
				if err != nil {
					return err
				}
				mutations = append(mutations, &backend.Mutation{Op: backend.MutationPut, Key: inodeKey, Value: inodeValue})
			}
			quotaMutations, err = e.reserveQuota(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      -inodeSizeDelta(inode.Size),
				Inodes:     -1,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		result = attemptResult
		mutations = append(mutations, &backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[1]), Value: parentValue})
		if len(quotaMutations) == 0 {
			return e.commitWithMetadataPredicatesAndWatch(ctx, plan.Kind, mount, plan.PrimaryKey, predicates, mutations, []backend.WatchEvent{watchEvent}, startVersion, commitVersion)
		}
		return e.commitWithoutMetadataPredicatesAndWatch(ctx, plan.Kind, mount, plan.PrimaryKey, mutations, []backend.WatchEvent{watchEvent}, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return model.RemoveResult{}, err
	}
	return result, nil
}

// Unlink removes one non-directory dentry, decrements its inode link count,
// and deletes the inode record when the last dentry goes away.
func (e *Executor) Unlink(ctx context.Context, req model.UnlinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileUnlinkProgram(req, mount, compile.WithQuotaMode(e.quotaMode()))
	if err != nil {
		return err
	}
	_, err = e.removeDentry(ctx, mount, program.Compiled, removeDentryRequest(req))
	return err
}

// RemoveDirectory removes one empty directory dentry and its directory inode.
// Empty is checked through the directory inode ChildCount, which every child
// membership mutation updates in the same metadata command.
func (e *Executor) RemoveDirectory(ctx context.Context, req model.RemoveDirectoryRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRemoveDirectoryProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	plan := delta.Plan
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		parent, err := e.readDirectoryInode(ctx, mount, req.Parent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := decrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := layout.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		dentry, err := e.readDentrySnapshot(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		record := dentry.record
		if record.Type != model.InodeTypeDirectory {
			return model.ErrInvalidRequest
		}
		inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return model.ErrNotFound
		}
		if inode.Type != model.InodeTypeDirectory || inode.ChildCount != 0 || inode.Inode == model.RootInode {
			return model.ErrInvalidRequest
		}
		childDentryPrefix, err := layout.EncodeDentryPrefix(mount, inode.Inode)
		if err != nil {
			return err
		}
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.Parent,
			Bytes:      -inodeSizeDelta(inode.Size),
			Inodes:     -1,
		}}, startVersion)
		if err != nil {
			return err
		}
		inodeKey, err := layout.EncodeInodeKey(mount, inode.Inode)
		if err != nil {
			return err
		}
		inodeValue, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		parentLinkDelete, err := parentIndexDeleteMutation(mount, record)
		if err != nil {
			return err
		}
		watchEvent, err := dentryWatchEvent(mount, backend.WatchOperationDelete, record)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{
			{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[0]), Value: parentValue},
			{Op: backend.MutationDelete, Key: cloneBytes(plan.MutateKeys[1])},
			{Op: backend.MutationDelete, Key: inodeKey},
			parentLinkDelete,
		}
		mutations = append(mutations, quotaMutations...)
		predicates := []*backend.Predicate{
			metadataValueEqualsPredicate(parent.key, parent.value),
			metadataValueEqualsPredicate(plan.PrimaryKey, dentry.value),
			metadataValueEqualsPredicate(inodeKey, inodeValue),
			metadataPrefixEmptyPredicate(childDentryPrefix),
		}
		return e.commitWithMetadataPredicatesAndWatch(ctx, plan.Kind, mount, plan.PrimaryKey, predicates, mutations, []backend.WatchEvent{watchEvent}, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	return nil
}

// Remove is the product-facing primitive for removing one non-directory
// namespace entry. Directory removal needs a separate directory-emptiness
// contract, so v1 keeps directory targets invalid instead of orphaning children.
func (e *Executor) Remove(ctx context.Context, req model.RemoveRequest) (model.RemoveResult, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.RemoveResult{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRemoveProgram(req, mount, compile.WithQuotaMode(e.quotaMode()))
	if err != nil {
		return model.RemoveResult{}, err
	}
	return e.removeDentry(ctx, mount, program.Compiled, removeDentryRequest(req))
}
