// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// Create creates one dentry and its inode record in a single transaction.
func (e *Executor) Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	if e.inodes == nil {
		return fsmeta.CreateResult{}, errInodeAllocatorRequired
	}
	if _, err := fsmeta.EncodeInodeValue(req.Attrs.InodeRecord(fsmeta.RootInode)); err != nil {
		return fsmeta.CreateResult{}, err
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	mount := mountRecord.Identity()
	// Allocate after cheap semantic validation and mount admission. Transaction
	// retries below reuse this single ID; failed creates may leave coordinator
	// ID gaps, but they cannot publish a different inode on retry.
	inodeID, err := e.inodes.AllocateCreateInode(ctx, mount, req.Parent, req.Name)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	program, err := compile.CompileCreateProgram(req, mount, inodeID, compile.WithQuotaMode(e.perasQuotaMode()))
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.CreateResult{}, err
	}
	plan := delta.Plan
	inode := req.Attrs.InodeRecord(inodeID)
	dentry := fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inodeID,
		Type:   inode.Type,
	}
	dentryValue := delta.WriteEffects[0].Value
	inodeValue := delta.WriteEffects[1].Value
	e.createTotal.Add(1)
	quotaChanges := []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.Parent,
		Bytes:      inodeSizeDelta(inode.Size),
		Inodes:     1,
	}}
	quotaOK := true
	if e.perasCommitter != nil && e.perasAuthority != nil && delta.Eligibility == compile.EligibilityVisibleCommit {
		var err error
		quotaOK, err = e.perasQuotaAllowsVisibleCommit(ctx, quotaChanges)
		if err != nil {
			return fsmeta.CreateResult{}, err
		}
	}
	if quotaOK {
		materialized := compile.MaterializedOp{CompiledOp: program.Compiled}
		if committed, err := e.tryPerasVisibleCommit(ctx, materialized); committed || err != nil {
			if err != nil {
				return fsmeta.CreateResult{}, err
			}
			e.rememberPerasCreate(mount, plan, inode)
			e.forgetPerasEmptyDirectory(mount, req.Parent)
			e.invalidateNegative(plan.MutateKeys[0])
			e.invalidateDirPages(req.Mount, req.Parent)
			return fsmeta.CreateResult{Dentry: dentry, Inode: inode}, nil
		}
	}
	mutations := []*kvrpcpb.Mutation{
		{
			Op:                kvrpcpb.Mutation_Put,
			Key:               cloneBytes(plan.MutateKeys[0]),
			Value:             dentryValue,
			AssertionNotExist: true,
		},
		{
			Op:                kvrpcpb.Mutation_Put,
			Key:               cloneBytes(plan.MutateKeys[1]),
			Value:             inodeValue,
			AssertionNotExist: true,
		},
	}
	predicates := []*kvrpcpb.AtomicPredicate{atomicNotExists(plan.MutateKeys[0]), atomicNotExists(plan.MutateKeys[1])}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
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
			// One-phase counters are per transaction attempt, not per logical
			// Create, so contention retries and admission misses stay visible.
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, all, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, all, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return fsmeta.CreateResult{}, err
	}
	// The new dentry replaces a previously-missing key; drop any negative
	// memo a prior Lookup may have planted, forget any Peras-derived empty
	// directory fact, and bump the parent's dirpage epoch so a stale
	// ReadDirPlus result cannot mask the new entry.
	e.rememberPerasCreate(mount, plan, inode)
	e.forgetPerasEmptyDirectory(mount, req.Parent)
	e.invalidateNegative(plan.MutateKeys[0])
	e.invalidateDirPages(req.Mount, req.Parent)
	return fsmeta.CreateResult{Dentry: dentry, Inode: inode}, nil
}
