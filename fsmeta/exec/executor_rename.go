// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type renameMove struct {
	mount      model.MountID
	identity   model.MountIdentity
	fromParent model.InodeID
	fromName   string
	toParent   model.InodeID
	toName     string
}

func renameMoveFromRename(req model.RenameRequest, identity model.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

func renameMoveFromRenameReplace(req model.RenameReplaceRequest, identity model.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

func renameMoveFromRenameSubtree(req model.RenameSubtreeRequest, identity model.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

// Rename moves one dentry inside the same subtree authority. It is deliberately
// a data-plane metadata command: no rooted handoff is published, so common staged
// publish paths do not serialize through the control plane.
func (e *Executor) Rename(ctx context.Context, req model.RenameRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRenameProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.requireSameAuthority(ctx, req.Mount, req.FromParent, req.ToParent); err != nil {
		return err
	}
	plan := delta.Plan
	move := renameMoveFromRename(req, mount)
	var movedSize uint64
	var movedInode bool
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		mutations, predicates, err := e.prepareRenameMutations(ctx, plan, move, startVersion, commitVersion, &movedSize, &movedInode)
		if err != nil {
			return err
		}
		if len(predicates) != 0 {
			return e.commitWithMetadataPredicates(ctx, plan.Kind, mount, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.commitWithoutMetadataPredicates(ctx, plan.Kind, mount, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	return nil
}

// RenameReplace atomically publishes the source dentry at the destination name,
// replacing an existing non-directory destination dentry when present. It is the
// artifact publish primitive: no subtree handoff or durability barrier is
// emitted, and all namespace/index mutations commit in one backend metadata command.
func (e *Executor) RenameReplace(ctx context.Context, req model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRenameReplaceProgram(req, mount)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	delta := program.Compiled.Delta
	if err := e.requireSameAuthority(ctx, req.Mount, req.FromParent, req.ToParent); err != nil {
		return model.RenameReplaceResult{}, err
	}
	plan := delta.Plan
	move := renameMoveFromRenameReplace(req, mount)
	var result model.RenameReplaceResult
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		nextResult, mutations, err := e.prepareRenameReplaceMutations(ctx, plan, move, startVersion, commitVersion)
		if err != nil {
			return fmt.Errorf("prepare rename replace: %w", err)
		}
		if err := e.commitWithoutMetadataPredicates(ctx, plan.Kind, mount, plan.PrimaryKey, mutations, startVersion, commitVersion); err != nil {
			return fmt.Errorf("commit rename replace: %w", err)
		}
		result = nextResult
		return nil
	}, delta.Authority); err != nil {
		return model.RenameReplaceResult{}, err
	}
	return result, nil
}

// RenameSubtree moves the subtree root dentry from source to destination.
// Descendants follow through inode parent links rather than key rewrites.
func (e *Executor) RenameSubtree(ctx context.Context, req model.RenameSubtreeRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRenameSubtreeProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	plan := delta.Plan
	authorityRoot := mountRecord.RootInode
	if e.subtrees != nil && authorityRoot == 0 {
		return model.ErrInvalidInodeID
	}
	var movedSize uint64
	var movedInode bool
	var committedAt uint64
	var handoffStarted bool
	move := renameMoveFromRenameSubtree(req, mount)
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		mutations, _, err := e.prepareRenameMutations(ctx, plan, move, startVersion, commitVersion, &movedSize, &movedInode)
		if err != nil {
			return err
		}
		if err := e.startSubtreeHandoff(ctx, req.Mount, authorityRoot, commitVersion); err != nil {
			return err
		}
		handoffStarted = true
		result, mutationErr := e.commitMetadataCommandAt(ctx, mount, plan.PrimaryKey, nil, mutations, startVersion, commitVersion)
		actualCommitVersion := result.CommitVersion
		// Subtree handoff start publishes a rooted predecessor frontier before the
		// data mutation runs. That external frontier must be the same commit_ts
		// used by the metadata command; otherwise concurrent handoffs can observe a
		// later completed frontier and reject the older pending handoff.
		// Once StartSubtreeHandoff is rooted, a Mutate error may still be
		// ambiguous with respect to primary commit. Complete closes the rooted
		// pending state; at worst this advances an empty era rather than leaving
		// an unrecoverable handoff.
		completeErr := e.completeSubtreeHandoff(ctx, req.Mount, authorityRoot, actualCommitVersion)
		if mutationErr != nil {
			if completeErr != nil {
				return errors.Join(mutationErr, fmt.Errorf("complete subtree handoff: %w", completeErr))
			}
			return mutationErr
		}
		if completeErr != nil {
			return completeErr
		}
		committedAt = actualCommitVersion
		return nil
	}, delta.Authority); err != nil {
		return err
	}
	if handoffStarted && committedAt == 0 {
		return errSubtreeHandoffWithoutFrontier
	}
	return nil
}

func (e *Executor) prepareRenameReplaceMutations(ctx context.Context, plan layout.OperationPlan, move renameMove, startVersion, commitVersion uint64) (model.RenameReplaceResult, []*backend.Mutation, error) {
	sourceDentry, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
	if err != nil {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("source dentry: %w", err)
	}
	if sourceDentry.Type == model.InodeTypeDirectory {
		return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
	}
	sourceInode, ok, err := e.readInode(ctx, move.identity, sourceDentry.Inode, startVersion)
	if err != nil {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("source inode: %w", err)
	}
	if !ok {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("%w: inode %d", model.ErrNotFound, sourceDentry.Inode)
	}
	if err := validateRenameReplaceDentryInode(sourceDentry, sourceInode); err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	if sourceInode.Type == model.InodeTypeDirectory {
		return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
	}
	fromParent, err := e.readDirectoryInode(ctx, move.identity, move.fromParent, startVersion)
	if err != nil {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("from parent inode: %w", err)
	}

	result := model.RenameReplaceResult{}
	destinationExisted := false
	destinationDentry, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion)
	if err == nil {
		destinationExisted = true
		result.Replaced = true
		result.OldDentry = destinationDentry
	} else if !errors.Is(err, model.ErrNotFound) {
		return model.RenameReplaceResult{}, nil, err
	} else {
		err = nil
	}

	var destinationInode model.InodeRecord
	if destinationExisted {
		if destinationDentry.Type == model.InodeTypeDirectory {
			return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
		}
		destinationInode, ok, err = e.readInode(ctx, move.identity, destinationDentry.Inode, startVersion)
		if err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		if !ok {
			return model.RenameReplaceResult{}, nil, fmt.Errorf("%w: inode %d", model.ErrNotFound, destinationDentry.Inode)
		}
		if err := validateRenameReplaceDentryInode(destinationDentry, destinationInode); err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		if destinationInode.Type == model.InodeTypeDirectory {
			return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
		}
		result.OldInode = destinationInode
	}
	toParent := fromParent
	if move.fromParent != move.toParent {
		toParent, err = e.readDirectoryInode(ctx, move.identity, move.toParent, startVersion)
		if err != nil {
			return model.RenameReplaceResult{}, nil, fmt.Errorf("to parent inode: %w", err)
		}
	}
	nextFromParent := fromParent.record
	nextToParent := toParent.record
	switch {
	case move.fromParent == move.toParent && destinationExisted:
		nextFromParent, err = decrementDirectoryChildCount(nextFromParent)
	case move.fromParent != move.toParent:
		nextFromParent, err = decrementDirectoryChildCount(nextFromParent)
		if err == nil && !destinationExisted {
			nextToParent, err = incrementDirectoryChildCount(nextToParent)
		}
	}
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}

	replacementDentry := sourceDentry
	replacementDentry.Parent = move.toParent
	replacementDentry.Name = move.toName
	replacementValue, err := encodeDentryValueForCommit(replacementDentry, sourceInode, true, commitVersion)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	putDestination := &backend.Mutation{
		Op:    backend.MutationPut,
		Key:   cloneBytes(plan.MutateKeys[1]),
		Value: replacementValue,
	}
	if !destinationExisted {
		putDestination.AssertionNotExist = true
	}
	fromParentValue, err := layout.EncodeInodeValue(nextFromParent)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	toParentValue, err := layout.EncodeInodeValue(nextToParent)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	mutations := []*backend.Mutation{
		{
			Op:  backend.MutationDelete,
			Key: cloneBytes(plan.MutateKeys[0]),
		},
		putDestination,
	}
	sourceParentDelete, err := parentIndexDeleteMutation(move.identity, sourceDentry)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	replacementParentPut, err := parentIndexPutMutation(move.identity, replacementDentry, !destinationExisted)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	mutations = append(mutations, sourceParentDelete, replacementParentPut)
	if destinationExisted && destinationDentry.Inode != replacementDentry.Inode {
		destinationParentDelete, err := parentIndexDeleteMutation(move.identity, destinationDentry)
		if err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		mutations = append(mutations, destinationParentDelete)
	}
	if move.fromParent == move.toParent {
		mutations = append(mutations, &backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue})
	} else {
		mutations = append(mutations,
			&backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue},
			&backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[3]), Value: toParentValue},
		)
	}

	quotaChanges := make([]QuotaChange, 0, 3)
	if move.fromParent != move.toParent {
		quotaChanges = append(quotaChanges,
			QuotaChange{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.fromParent, Bytes: -inodeSizeDelta(sourceInode.Size), Inodes: -1},
			QuotaChange{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.toParent, Bytes: inodeSizeDelta(sourceInode.Size), Inodes: 1},
		)
	}
	if destinationExisted {
		destinationInodeKey, err := layout.EncodeInodeKey(move.identity, destinationInode.Inode)
		if err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		if destinationInode.Inode == sourceInode.Inode && destinationInode.LinkCount <= 1 {
			return model.RenameReplaceResult{}, nil, model.ErrInvalidValue
		}
		if destinationInode.LinkCount <= 1 {
			result.OldInodeDeleted = true
			mutations = append(mutations, &backend.Mutation{
				Op:  backend.MutationDelete,
				Key: cloneBytes(destinationInodeKey),
			})
		} else {
			destinationInode.LinkCount--
			destinationValue, err := layout.EncodeInodeValue(destinationInode)
			if err != nil {
				return model.RenameReplaceResult{}, nil, err
			}
			mutations = append(mutations, &backend.Mutation{
				Op:    backend.MutationPut,
				Key:   cloneBytes(destinationInodeKey),
				Value: destinationValue,
			})
		}
		quotaChanges = append(quotaChanges, QuotaChange{
			Mount:      move.mount,
			MountKeyID: move.identity.MountKeyID,
			Scope:      move.toParent,
			Bytes:      -inodeSizeDelta(result.OldInode.Size),
			Inodes:     -1,
		})
	}
	if len(quotaChanges) > 0 {
		quotaMutations, err := e.reserveQuota(ctx, quotaChanges, startVersion)
		if err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		mutations = append(mutations, quotaMutations...)
	}
	return result, mutations, nil
}

func validateRenameReplaceDentryInode(dentry model.DentryRecord, inode model.InodeRecord) error {
	if dentry.Inode != inode.Inode {
		return fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
	}
	if dentry.Type != inode.Type {
		return fmt.Errorf("%w: dentry type=%s inode type=%s", model.ErrInvalidValue, dentry.Type, inode.Type)
	}
	return nil
}

func (e *Executor) prepareRenameMutations(ctx context.Context, plan layout.OperationPlan, move renameMove, startVersion, commitVersion uint64, movedSize *uint64, movedInode *bool) ([]*backend.Mutation, []*backend.Predicate, error) {
	sourceDentry, err := e.readDentrySnapshot(ctx, plan.ReadKeys[0], startVersion)
	if err != nil {
		return nil, nil, err
	}
	sourceRecord := sourceDentry.record
	if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
		return nil, nil, model.ErrExists
	} else if !errors.Is(err, model.ErrNotFound) {
		return nil, nil, err
	}
	record := sourceRecord
	record.Parent = move.toParent
	record.Name = move.toName
	*movedSize = 0
	*movedInode = false
	var inode model.InodeRecord
	var inodeOK bool
	if inode, inodeOK, err = e.readInode(ctx, move.identity, record.Inode, startVersion); err != nil {
		return nil, nil, err
	} else if inodeOK {
		*movedSize = inode.Size
		*movedInode = true
	}
	value, err := encodeDentryValueForCommit(record, inode, inodeOK, commitVersion)
	if err != nil {
		return nil, nil, err
	}
	fromParent, err := e.readDirectoryInode(ctx, move.identity, move.fromParent, startVersion)
	if err != nil {
		return nil, nil, err
	}
	toParent := fromParent
	if move.fromParent != move.toParent {
		nextFrom, err := decrementDirectoryChildCount(fromParent.record)
		if err != nil {
			return nil, nil, err
		}
		fromParent.record = nextFrom
		toParent, err = e.readDirectoryInode(ctx, move.identity, move.toParent, startVersion)
		if err != nil {
			return nil, nil, err
		}
		nextTo, err := incrementDirectoryChildCount(toParent.record)
		if err != nil {
			return nil, nil, err
		}
		toParent.record = nextTo
	}
	fromParentValue, err := layout.EncodeInodeValue(fromParent.record)
	if err != nil {
		return nil, nil, err
	}
	toParentValue, err := layout.EncodeInodeValue(toParent.record)
	if err != nil {
		return nil, nil, err
	}
	mutations := []*backend.Mutation{
		{
			Op:  backend.MutationDelete,
			Key: cloneBytes(plan.MutateKeys[0]),
		},
		{
			Op:                backend.MutationPut,
			Key:               cloneBytes(plan.MutateKeys[1]),
			Value:             value,
			AssertionNotExist: true,
		},
	}
	sourceParentDelete, err := parentIndexDeleteMutation(move.identity, sourceRecord)
	if err != nil {
		return nil, nil, err
	}
	destinationParentPut, err := parentIndexPutMutation(move.identity, record, true)
	if err != nil {
		return nil, nil, err
	}
	mutations = append(mutations, sourceParentDelete, destinationParentPut)
	if move.fromParent == move.toParent {
		mutations = append(mutations, &backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue})
	} else {
		mutations = append(mutations,
			&backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue},
			&backend.Mutation{Op: backend.MutationPut, Key: cloneBytes(plan.MutateKeys[3]), Value: toParentValue},
		)
	}
	if *movedInode {
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{
			{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.fromParent, Bytes: -inodeSizeDelta(*movedSize), Inodes: -1},
			{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.toParent, Bytes: inodeSizeDelta(*movedSize), Inodes: 1},
		}, startVersion)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, quotaMutations...)
	}
	predicates := []*backend.Predicate{
		metadataValueEqualsPredicate(plan.ReadKeys[0], sourceDentry.value),
		metadataNotExistsPredicate(plan.ReadKeys[1]),
		metadataNotExistsPredicate(destinationParentPut.Key),
		metadataValueEqualsPredicate(fromParent.key, fromParent.value),
	}
	if move.fromParent != move.toParent {
		predicates = append(predicates, metadataValueEqualsPredicate(toParent.key, toParent.value))
	}
	return mutations, predicates, nil
}

func (e *Executor) startSubtreeHandoff(ctx context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if e == nil || e.subtrees == nil || mount == "" || root == 0 || frontier == 0 {
		return nil
	}
	return e.subtrees.StartSubtreeHandoff(ctx, mount, root, frontier)
}

func (e *Executor) completeSubtreeHandoff(ctx context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if e == nil || e.subtrees == nil || mount == "" || root == 0 || frontier == 0 {
		return nil
	}
	return e.subtrees.CompleteSubtreeHandoff(ctx, mount, root, frontier)
}

func (e *Executor) requireSameAuthority(ctx context.Context, mount model.MountID, fromParent, toParent model.InodeID) error {
	if e == nil || e.authorities == nil {
		return nil
	}
	same, err := e.authorities.SameAuthority(ctx, mount, fromParent, toParent)
	if err != nil {
		return err
	}
	if !same {
		return model.ErrCrossAuthorityRename
	}
	return nil
}
