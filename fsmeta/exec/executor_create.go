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

// Create creates one dentry and its inode record in one metadata command.
func (e *Executor) Create(ctx context.Context, req model.CreateRequest) (model.CreateResult, error) {
	if e.inodes == nil {
		return model.CreateResult{}, errInodeAllocatorRequired
	}
	if _, err := layout.EncodeInodeValue(req.Attrs.InodeRecord(model.RootInode)); err != nil {
		return model.CreateResult{}, err
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.CreateResult{}, err
	}
	mount := mountRecord.Identity()
	// Allocate after cheap semantic validation and mount admission. Transaction
	// retries below reuse this single ID; failed creates may leave coordinator
	// ID gaps, but they cannot publish a different inode on retry.
	inodeID, err := e.inodes.AllocateCreateInode(ctx, mount, req.Parent, req.Name)
	if err != nil {
		return model.CreateResult{}, err
	}
	program, err := compile.CompileCreateProgram(req, mount, inodeID, compile.WithQuotaMode(e.quotaMode()))
	if err != nil {
		return model.CreateResult{}, err
	}
	delta := program.Compiled.Delta
	plan := delta.Plan
	inode := req.Attrs.InodeRecord(inodeID)
	dentry := model.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inodeID,
		Type:   inode.Type,
	}
	inodeValue := delta.WriteEffects[2].Value
	e.createTotal.Add(1)
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		parent, err := e.readDirectoryInode(ctx, mount, req.Parent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := incrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := layout.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		dentryValue, err := encodeDentryValueForCommit(dentry, inode, true, commitVersion)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{
			{
				Op:    backend.MutationPut,
				Key:   cloneBytes(plan.MutateKeys[0]),
				Value: parentValue,
			},
			{
				Op:                backend.MutationPut,
				Key:               cloneBytes(plan.MutateKeys[1]),
				Value:             dentryValue,
				AssertionNotExist: true,
			},
			{
				Op:                backend.MutationPut,
				Key:               cloneBytes(plan.MutateKeys[2]),
				Value:             inodeValue,
				AssertionNotExist: true,
			},
		}
		parentLink, err := parentIndexPutMutation(mount, dentry, true)
		if err != nil {
			return err
		}
		mutations = append(mutations, parentLink)
		predicates := []*backend.Predicate{
			metadataValueEqualsPredicate(parent.key, parent.value),
			metadataNotExistsPredicate(plan.MutateKeys[1]),
			metadataNotExistsPredicate(plan.MutateKeys[2]),
			metadataNotExistsPredicate(parentLink.Key),
		}
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.Parent,
			Bytes:      inodeSizeDelta(inode.Size),
			Inodes:     1,
		}}, startVersion)
		if err != nil {
			return err
		}
		all := append(cloneMutations(mutations), quotaMutations...)
		if len(quotaMutations) == 0 {
			// One-phase counters are per commit attempt, not per logical
			// Create, so contention retries and admission misses stay visible.
			return e.commitWithMetadataPredicates(ctx, plan.Kind, mount, plan.PrimaryKey, predicates, all, startVersion, commitVersion)
		}
		return e.commitWithoutMetadataPredicates(ctx, plan.Kind, mount, plan.PrimaryKey, all, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return model.CreateResult{}, err
	}
	return model.CreateResult{Dentry: dentry, Inode: inode}, nil
}
