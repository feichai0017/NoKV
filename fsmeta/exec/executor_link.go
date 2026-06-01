// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// Link creates a second dentry for an existing non-directory inode and bumps
// the inode link count in the same metadata command.
func (e *Executor) Link(ctx context.Context, req model.LinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileLinkProgram(req, mount, compile.WithQuotaMode(e.quotaMode()))
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	plan := delta.Plan
	if err := e.withCommitRetry(ctx, func(startVersion, commitVersion uint64) error {
		sourceDentry, err := e.readDentrySnapshot(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		record := sourceDentry.record
		if record.Type == model.InodeTypeDirectory {
			return model.ErrInvalidRequest
		}
		if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
			return model.ErrExists
		} else if !errors.Is(err, model.ErrNotFound) {
			return err
		}
		inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return model.ErrNotFound
		}
		if inode.Type == model.InodeTypeDirectory {
			return model.ErrInvalidRequest
		}
		if inode.LinkCount == ^uint32(0) {
			return model.ErrInvalidRequest
		}
		if inode.LinkCount == 0 {
			inode.LinkCount = 1
		}
		oldInode := inode
		parent, err := e.readDirectoryInode(ctx, mount, req.ToParent, startVersion)
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
		oldInodeValue, err := layout.EncodeInodeValue(oldInode)
		if err != nil {
			return err
		}
		inode.LinkCount++

		newDentry := model.DentryRecord{
			Parent: req.ToParent,
			Name:   req.ToName,
			Inode:  record.Inode,
			Type:   record.Type,
		}
		dentryValue, err := encodeDentryValueForCommit(newDentry, inode, true, commitVersion)
		if err != nil {
			return err
		}
		sourceValue, err := encodeDentryValueForCommit(record, inode, true, commitVersion)
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
		mutations := []*backend.Mutation{
			{
				Op:    backend.MutationPut,
				Key:   cloneBytes(plan.ReadKeys[0]),
				Value: sourceValue,
			},
			{
				Op:                backend.MutationPut,
				Key:               cloneBytes(plan.ReadKeys[1]),
				Value:             dentryValue,
				AssertionNotExist: true,
			},
			{
				Op:    backend.MutationPut,
				Key:   inodeKey,
				Value: inodeValue,
			},
			{
				Op:    backend.MutationPut,
				Key:   cloneBytes(plan.MutateKeys[1]),
				Value: parentValue,
			},
		}
		parentLink, err := parentIndexPutMutation(mount, newDentry, true)
		if err != nil {
			return err
		}
		mutations = append(mutations, parentLink)
		parentProjection, parentProjectionPredicate, err := e.directoryDentryProjectionMutation(ctx, mount, nextParent, startVersion, commitVersion)
		if err != nil {
			return err
		}
		if parentProjection != nil {
			mutations = append(mutations, parentProjection)
		}
		pathIndex, err := e.pathIndexPutMutations(ctx, mount, newDentry, startVersion, commitVersion)
		if err != nil {
			return err
		}
		mutations = append(mutations, pathIndex...)
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.ToParent,
			Bytes:      inodeSizeDelta(inode.Size),
			Inodes:     1,
		}}, startVersion)
		if err != nil {
			return err
		}
		mutations = append(mutations, quotaMutations...)
		sourceWatch, err := dentryWatchEvent(mount, backend.WatchOperationUpdate, record)
		if err != nil {
			return err
		}
		linkWatch, err := dentryWatchEvent(mount, backend.WatchOperationLink, newDentry)
		if err != nil {
			return err
		}
		watchEvents := []backend.WatchEvent{sourceWatch, linkWatch}
		if len(quotaMutations) == 0 {
			// Link is safe on 1PC only when the source dentry and inode still
			// equal the records read by this attempt. These value predicates are
			// the correctness boundary that prevents overwriting a concurrent
			// UpdateInode with an older inode body.
			predicates := []*backend.Predicate{
				metadataValueEqualsPredicate(plan.ReadKeys[0], sourceDentry.value),
				metadataNotExistsPredicate(plan.ReadKeys[1]),
				metadataValueEqualsPredicate(inodeKey, oldInodeValue),
				metadataValueEqualsPredicate(parent.key, parent.value),
				metadataNotExistsPredicate(parentLink.Key),
			}
			if parentProjectionPredicate != nil {
				predicates = append(predicates, parentProjectionPredicate)
			}
			return e.commitWithMetadataPredicatesAndWatch(ctx, plan.Kind, mount, plan.PrimaryKey, predicates, mutations, watchEvents, startVersion, commitVersion)
		}
		return e.commitWithoutMetadataPredicatesAndWatch(ctx, plan.Kind, mount, plan.PrimaryKey, mutations, watchEvents, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	return nil
}
